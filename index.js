// var so we can rewire it in tests
var exploranda = require('exploranda-core');
const async = require('async')
const _ = require('lodash');
const uuid = require('uuid')

const apiConfig = {
  region: process.env.AWS_REGION
}

const defaults = {
  MAX_RECURSION_DEPTH: 3
}

function log(level, message) {
  if (process.env.DONUT_DAYS_DEBUG || level === 'ERROR' || level === "WARN") {
    console.log(`${level}\t${message}`)
  }
}

const trace = _.partial(log, 'TRACE')
const debug = _.partial(log, 'DEBUG')
const info = _.partial(log, 'INFO')
const warn = _.partial(log, 'WARN')
const error = _.partial(log, 'ERROR')

// If this signature changes, remember to update the test harness or tests will break.
function transformInput(stage, stageConfig, processParams) {
  trace(`making input for ${stage} with ${JSON.stringify(stageConfig)}`)
  return processParams(stageConfig)
}

const builtInTransformations = {
  uuid: () => uuid.v4(),
  matches: ({a, b}) => a === b,
  env: ({varName}) => process.env[varName],
  isEmptyList: ({list}) => _.isArray(list) && list.length === 0,
  isNonEmptyList: ({list}) => _.isArray(list) && list.length !== 0,
  slice: ({list, start, end}) => _.slice(list, start, end),
  toJson: (p) => JSON.stringify(p),
  fromJson: ({string}) => JSON.parse(string),
  qualifiedDependencyName: ({configStepName, dependencyName}) => getQualifiedDepName(configStepName, dependencyName),
}

function processParams(helperFunctions, input, params) {
  transformers = {...builtInTransformations, ...helperFunctions}
  const output = {}
  _.each(params, (v, k) => {
    output[k] = processParamValue(helperFunctions, input, v)
    trace(`Processed param [ name: ${k} ] [ plan: ${v ? JSON.stringify(v) : v} ] [ input: ${_.isObjectLike(input) ? JSON.stringify(input) : input} ] [ result: ${_.isObjectLike(output[k]) ? JSON.stringify(output[k]) : output[k]} ]`)
  })
  return output
}

function processParamValue(helperFunctions, input, value) {
  if (value.value) {
    return value.value
  } else if (value.ref) {
    return _.get(input, value.ref)
  } else if (value.every) {
    return _(processParams(helperFunctions, input, value.every)).values().every()
  } else if (value.not) {
    return !processParamValue(helperFunctions, input, value.not)
  } else if (value.some) {
    return _(processParams(helperFunctions, input, value.some)).values().some()
  } else if (value.or) {
    return _(value.or).map(_.partial(processParamValue, helperFunctions, input)).find()
  } else if (value.all) {
    return processParams(helperFunctions, input, value.all)
  } else if (value.helper) {
    const helper = transformers[value.helper]
    if (!_.isFunction(helper)) {
      error(`No helper function named ${value.helper}. Instead found ${safeStringify(helper)}. Available helpers: ${JSON.stringify(_.keys(transformers))}`)
    } else {
      return transformers[value.helper](processParams(helperFunctions, input, value.params))
    }
  }
}

function safeStringify(o) {
  if (_.isObjectLike(o)) {
    return JSON.stringify(o)
  } else {
    return o
  }
}

function dependencyBuilders(helpers) { 
  return {
    ...{
      invokeFunction: (params, addDependency) => {
        addDependency('invoke',  {
          accessSchema: exploranda.dataSources.AWS.lambda.invoke,
          params: {
            FunctionName: {
              value: params.FunctionName
            },
            InvocationType: {value: 'Event'},
            Payload: {
              value: params.Payload
            }
          }
        }, params.dryRun)
      },
      recurse: (params, addDependency, getDependencyName, processParams, processParamValue) => {
        const recursionDepth = (processParamValue({ref: 'event.recursionDepth'}) || 1) + 1
        const allowedRecursionDepth = processParamValue({ref: 'config.overrides.MAX_RECURSION_DEPTH'}) || defaults.MAX_RECURSION_DEPTH
        if (allowedRecursionDepth > recursionDepth) {
          addDependency('invoke',  {
            accessSchema: exploranda.dataSources.AWS.lambda.invoke,
            params: {
              FunctionName: {
                value: processParamValue({ref: 'context.invokedFunctionArn'})
              },
              InvocationType: {value: 'Event'},
              Payload: {
                value: JSON.stringify({...params.Payload, ...{
                  recursionDepth: (processParamValue({ref: 'event.recursionDepth'}) || 1) + 1
                }})
            }}}, params.dryRun)
        } else {
          error(`Max recursion depth exceeded. [ depth: ${recursionDepth} ] [ allowedRecursionDepth: ${allowedRecursionDepth} ] [ params: ${safeStringify(params)} ]`)
        }
      },
      exploranda: (params, addDependency, getDependencyName, processParams) => {
        addDependency(params.dependencyName, {
          accessSchema: _.get(exploranda, params.accessSchema),
          params: processParams(params.params)
        }, params.dryRun)
      },
      storeItem: (params, addDependency) => {
        addDependency('stored', {
          accessSchema: {
            dataSource: 'SYNTHETIC',
            transformation: () => params.item
          },
          params: {}
        })
      }
    }, ...(helpers || {})
  }
}

function getQualifiedDepName(prefix, depName) {
  return `${prefix}_${depName}`
}

function generateDependencies(input, config, transformers, mergedDependencyBuilders) {
  const dependencies = {}
  function addDependency(prefix, depName, dep, dryRun) {
    const name = getQualifiedDepName(prefix, depName)
    if (dryRun) {
      console.log(`Would add dependency ${name} consisting of ${JSON.stringify(stringableDependency(dep))}`)
    } else {
      dependencies[name] = dep
    }
    return name
  }
  _.each(config, (desc, name) => {
    if (testEvent(name, desc.conditions, _.partial(processParams, transformers, input))) {
      builder = mergedDependencyBuilders[desc.action]
      return builder(
        processParams(transformers, input, desc.params),
        _.partial(addDependency, name),
        _.partial(getQualifiedDepName, name),
        _.partial(processParams, transformers, input),
        _.partial(processParamValue, transformers, input)
      )
    }
  })
  return dependencies
}

function stringableDependency(dep) {
  const stringable = _.cloneDeep(dep)
  stringable.accessSchema = {dataSource: _.get(dep, 'accessSchema.dataSource'), name: _.get(dep, 'accessSchema.name')}
  return stringable
}


const testEvent = function(name, conditions, processParams) {
  const notApplicable = !conditions
  const processed = processParams(conditions)
  const result = _.every(_.values(processed))
  trace(`Testing conditions for ${name}: [ conditions: ${conditions ? JSON.stringify(conditions) : conditions} ] [ processed: ${_.isObjectLike(processed) ? JSON.stringify(processed) : processed} ] [ result: ${result} ]`)
  return result
}

function logStage(stage, vars, dependencies) {
  trace(`${stage}: [ vars: ${_.isObjectLike(vars) ? JSON.stringify(vars) : vars} ] [ deps: ${JSON.stringify(_.reduce(dependencies, (acc, v, k) => {
    acc[k] = stringableDependency(v)
    return acc
  }, {}))} ]`)
}

// If this signature changes, remember to update the test harness or tests will break.
function createTask(config, helperFunctions, dependencyHelpers) {
  const mergedDependencyBuilders = dependencyBuilders(dependencyHelpers)
  const makeIntroDependencies = function(event, context) {
    const stageConfig = _.get(config, ['intro', 'transformers'])
    trace('intro')
    const input = transformInput("intro", stageConfig, _.partial(processParams, helperFunctions, {event, context, config}))
    return {vars: input, dependencies: generateDependencies({stage: input, event, context, config}, _.get(config, 'intro.dependencies'), helperFunctions, mergedDependencyBuilders)}
  }
  const makeMainDependencies = function(event, context, intro) {
    const stageConfig = _.get(config, ['main', 'transformers'])
    const input = transformInput("main", stageConfig, _.partial(processParams, helperFunctions, {event, context, intro, config}))
    return {vars: input, dependencies: generateDependencies({stage: input, event, context, intro, config}, _.get(config, 'main.dependencies'), helperFunctions, mergedDependencyBuilders)}
  }
  const makeOutroDependencies = function(event, context, intro, main) {
    const stageConfig = _.get(config, ['outro', 'transformers'])
    const input = transformInput("outro", stageConfig, _.partial(processParams, helperFunctions, {event, context, intro, main, config}))
    return {vars: input, dependencies: generateDependencies({stage: input, event, context, intro, main, config}, _.get(config, 'outro.dependencies'), helperFunctions, mergedDependencyBuilders)}
  }
  return function(event, context, callback) {
    info(`event: ${safeStringify(event)}`)
    function performIntro(callback) {
      const {vars, dependencies} = makeIntroDependencies(event, context)
      logStage('intro', vars, dependencies)
      const reporter = exploranda.Gopher(dependencies);
      reporter.report((e, n) => callback(e, {vars, results: n}));
    }
    function performMain(intro, callback) {
      const {vars, dependencies} = makeMainDependencies(event, context, intro)
      logStage('main', vars, dependencies)
      const reporter = exploranda.Gopher(dependencies);
      reporter.report((e, n) => callback(e, intro, {vars, results: n}));
    }
    function performOutro(intro, main, callback) {
      const {vars, dependencies} = makeOutroDependencies(event, context, intro, main)
      logStage('outro', vars, dependencies)
      const reporter = exploranda.Gopher(dependencies);
      reporter.report((e, n) => callback(e, intro, main, {vars, results: n}));
    }
    function performCleanup(intro, main, outro, callback) {
      setTimeout(() => {
        try {
    const stageConfig = _.get(config, ['cleanup', 'transformers'])
        callback(null, transformInput('cleanup', stageConfig,  _.partial(processParams, helperFunctions, {event, context, intro, main, outro})))
        } catch(e) {
          callback(e)
        }
      }, 0)
    }
    if (config.conditions, _.partial(processParams, helperFunctions, {event, context})) {
      debug(`event ${event ? JSON.stringify(event) : event} matched for processing`)
      async.waterfall([
        performIntro,
        performMain,
        performOutro,
        performCleanup,
      ], callback)
    } else {
      debug(`event ${event ? JSON.stringify(event) : event} did not match for processing`)
      callback()
    }
  }
}

module.exports = {
  createTask,
  exploranda
}
