// var so we can rewire it in tests
var exploranda = require('exploranda-core');
const async = require('async')
const _ = require('lodash');
const uuid = require('uuid')

const apiConfig = {
  region: process.env.AWS_REGION
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

function testConditionSet(name, conditions, input) {
  return _(conditions).map((condition) => testCondition(condition, input)).some()
}

const conditionTesters = {
  matchesAll: (matches, input) => _(matches).map((v, k) => {
    trace(`[ att: ${k} ] [ input: ${JSON.stringify(input)} ] [ val: ${_.get(input, k)} ] [ expect: ${v} ]`)
    return _.get(input, k) === v
  }).every(),
  matchesAny: (matches, input) => _(matches).map((v, k) => _.get(input, k) === v).some(),
  isEmptyList: (prop, input) => _.isArray(_.get(input, prop)) && _.get(input, prop).length === 0,
  isNonEmptyList: (prop, input) => _.isArray(_.get(input, prop)) && _.get(input, prop).length !== 0
}

function testCondition(condition, input) {
  return _(conditionTesters).map((v, k) => {
    const relevantCondition = _.get(condition, k)
    result = v(relevantCondition, input)
    trace(`[ condition: ${JSON.stringify(condition)} ] [ part: ${k} ] [ relevant: ${!!relevantCondition} ] [ result: ${!relevantCondition || result} ]`)
    return (!relevantCondition || result)
  }).every()
}

// If this signature changes, remember to update the test harness or tests will break.
function transformInput(stage, stageConfig, processParams) {
  trace(`making input for ${stage} with ${JSON.stringify(stageConfig)}`)
  return processParams(stageConfig)
}

const builtInTransformations = {
  uuid: () => uuid.v4(),
}

function processParams(helperFunctions, input, config) {
  transformers = {...builtInTransformations, ...helperFunctions}
  const output = {}
  _.each(config, (v, k) => {
    if (v.value) {
      output[k] = v.value
    } else if (v.ref) {
      output[k] = _.get(input, v.ref)
    } else if (v.all) {
      output[k] = processParams(helperFunctions, input, v.all)
    } else if (v.helper) {
      output[k] = transformers[v.helper](processParams(helperFunctions, input, v.params))
    }
  })
  return output
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
        })
      },
      exploranda: (params, addDependency, getDependencyName, processParams) => {
        addDependency(params.dependencyName, {
          accessSchema: _.get(exploranda, params.accessSchema),
          params: processParams(params.params)
        })
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

function generateDependencies(input, config, transformers, mergedDependencyBuilders) {
  const dependencies = {}
  function getQualifiedDepName(prefix, depName) {
    return `${prefix}_${depName}`
  }
  function addDependency(prefix, depName, dep) {
    const name = getQualifiedDepName(prefix, depName)
    dependencies[name] = dep
    return name
  }
  _.each(config, (desc, name) => {
    trace(`5 ${JSON.stringify(input)}`)
    if (testEvent(input, desc.conditions)) {
      builder = mergedDependencyBuilders[desc.action]
      return builder(
        processParams(transformers, input, desc.params),
        _.partial(addDependency, name),
        _.partial(getQualifiedDepName, name),
        _.partial(processParams, transformers, input)
      )
    }
  })
  return dependencies
}

const testEvent = function(input, conditions) {
  return !conditions || _(conditions).map((v, k) => {
    trace(`Testing conditionSet ${k} with input ${JSON.stringify(input)}`)
    const result = testConditionSet(k, v, input)
    trace(`Tested conditionSet ${k}. result: ${result}`)
    return result
  }).some()
}

// If this signature changes, remember to update the test harness or tests will break.
function createTask(config, helperFunctions, dependencyHelpers) {
  const mergedDependencyBuilders = dependencyBuilders(dependencyHelpers)
  const makeIntroDependencies = function(event, context) {
    const stageConfig = _.get(config, ['intro', 'transformers'])
    trace('intro')
    const input = transformInput("intro", stageConfig, _.partial(processParams, helperFunctions, {event, context}))
    return {vars: input, dependencies: generateDependencies({stage: input, event, context}, _.get(config, 'intro.dependencies'), helperFunctions, mergedDependencyBuilders)}
  }
  const makeMainDependencies = function(event, context, intro) {
    const stageConfig = _.get(config, ['main', 'transformers'])
    const input = transformInput("main", stageConfig, _.partial(processParams, helperFunctions, {event, context, intro}))
    return {vars: input, dependencies: generateDependencies({stage: input, event, context, intro}, _.get(config, 'main.dependencies'), helperFunctions, mergedDependencyBuilders)}
  }
  const makeOutroDependencies = function(event, context, intro, main) {
    const stageConfig = _.get(config, ['outro', 'transformers'])
    const input = transformInput("outro", stageConfig, _.partial(processParams, helperFunctions, {event, context, intro, main}))
    return {vars: input, dependencies: generateDependencies({stage: input, event, context, intro, main}, _.get(config, 'outro.dependencies'), helperFunctions, mergedDependencyBuilders)}
  }
  return function(event, context, callback) {
    function performIntro(callback) {
      const {vars, dependencies} = makeIntroDependencies(event, context)
      const reporter = exploranda.Gopher(dependencies);
      reporter.report((e, n) => callback(e, {vars, results: n}));
    }
    function performMain(intro, callback) {
      const {vars, dependencies} = makeMainDependencies(event, context, intro)
      const reporter = exploranda.Gopher(dependencies);
      reporter.report((e, n) => callback(e, intro, {vars, results: n}));
    }
    function performOutro(intro, main, callback) {
      const {vars, dependencies} = makeOutroDependencies(event, context, intro, main)
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
    if (testEvent({event, context}, config.conditions)) {
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
