// var so we can rewire it in tests
var exploranda = require('exploranda-core');
const async = require('async')
const _ = require('lodash');
const uuid = require('uuid')

const apiConfig = {
  region: process.env.AWS_REGION
}

function log(level, message) {
  if (process.env.DONUT_DAYS_DEBUG || level === 'ERROR') {
    console.log(`${level}\t${message}`)
  }
}

const trace = _.partial(log, 'TRACE')
const debug = _.partial(log, 'DEBUG')
const info = _.partial(log, 'INFO')
const error = _.partial(log, 'ERROR')

function testConditionSet(name, conditions, input) {
  return _(conditions).map((condition) => testCondition(condition, input)).some()
}

const conditionTesters = {
  matchesAll: (matches, input) => _(matches).map((v, k) => {
    trace(`[ att: ${k} ] [ input: ${JSON.stringify(input)} ] [ val: ${_.get(input, k)} ] [ expect: ${v} ]`)
    return _.get(input, k) === v
  }).every(),
  matchesAny: (matches, input) => _(matches).map((v, k) => _.get(input, k) === v).some()
}

function stageTransformers(helpers) {
  return {
    ...{
      uuid: (uuidConfig, source, dest) => _.each(uuidConfig, (v) => { dest[v] = uuid.v4() }),
        copy: (copyConfig, source, dest) => _.each(copyConfig, (v, k) => {
        trace(`${v}, ${k}, ${JSON.stringify(source)}, ${JSON.stringify(dest)}`)
        _.set(dest, v, _.get(source, k))
      })
    },
    ...(helpers || {})
  }
}

function testCondition(condition, input) {
  return _(conditionTesters).map((v, k) => {
    const relevantCondition = _.get(condition, k)
    result = v(relevantCondition, input)
    trace(`[ condition: ${JSON.stringify(condition)} ] [ part: ${k} ] [ relevant: ${!!relevantCondition} ] [ result: ${!relevantCondition || result} ]`)
    return (!relevantCondition || result)
  }).every()
}

function transformStage(transformations, transformationConfig, source, dest) {
  return _(transformations).each((v, k) => {
    _.each(transformationConfig, (transformation) => {
      const relevantTransformation = _.get(transformation, k)
      trace(`performing ${JSON.stringify(transformation)} : ${k} ${JSON.stringify(relevantTransformation)}`)
      if (relevantTransformation) {
        trace(`[ transform: ${k} ]`)
        v(relevantTransformation, source, dest)
      }
    })
  })
}

// If this signature changes, remember to update the test harness or tests will break.
function transformInput(transformations, stage, config, source) {
  const stageConfig = _.get(config, [stage, 'transformers'])
  trace(`making input for ${stage} with ${JSON.stringify(stage)}`)
  const newInput = {}
  _.each(stageConfig, (v, k) => {
    trace(`transforming ${k} with config ${JSON.stringify(v)}`) 
    transformStage(transformations, v, source, newInput)
  })
  trace(`input for ${stage}: ${JSON.stringify(newInput)}`)
  return newInput
}

function processParams(input, config, helperFunctions) {
  const output = {}
  _.each(config, (v, k) => {
    if (v.value) {
      output[k] = v.value
    } else if (v.ref) {
      output[k] = _.get(input, v.ref)
    } else if (v.helper) {
      output[k] = helperFunctions[v.helper](processParams(input, v.params, helperFunctions))
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
    if (testEvent(input, desc.conditions)) {
      builder = mergedDependencyBuilders[desc.action]
      return builder(
        processParams(input, desc.params, transformers),
        _.partial(addDependency, name),
        _.partial(getQualifiedDepName, name)
      )
    }
  })
  return dependencies
}

const testEvent = function(input, conditions) {
  return !conditions || _(conditions).map((v, k) => {
    trace(`Testing conditionSet ${k}`)
    const result = testConditionSet(k, v, input)
    trace(`Tested conditionSet ${k}. result: ${result}`)
    return result
  }).some()
}

// If this signature changes, remember to update the test harness or tests will break.
function createTask(config, helperFunctions, dependencyHelpers) {
  const transformers = stageTransformers(helperFunctions)
  const mergedDependencyBuilders = dependencyBuilders(dependencyHelpers)
  const makeIntroDependencies = function(event, context) {
    trace('intro')
    const input = transformInput(transformers, 'intro', config, {event, context})
    return {vars: input, dependencies: generateDependencies({stage: input, event, context}, _.get(config, 'intro.dependencies'), transformers, mergedDependencyBuilders)}
  }
  const makeMainDependencies = function(event, context, intro) {
    const input = transformInput(transformers, 'main', config, {event, context, intro})
    return {vars: input, dependencies: generateDependencies({stage: input, event, context, intro}, _.get(config, 'main.dependencies'), transformers, mergedDependencyBuilders)}
  }
  const makeOutroDependencies = function(event, context, intro, main) {
    const input = transformInput(transformers, 'outro', config, {event, context, intro, main})
    return {vars: input, dependencies: generateDependencies({stage: input, event, context, intro, main}, _.get(config, 'outro.dependencies'), transformers, mergedDependencyBuilders)}
  }
  return function(event, context, callback) {
    function performIntro(callback) {
      const {vars, dependencies} = makeIntroDependencies(event, context)
      const reporter = exploranda.Gopher(dependencies);
      reporter.report((e, n) => callback(e, {stage: vars, results: n}));
    }
    function performMain(intro, callback) {
      const {vars, dependencies} = makeMainDependencies(event, context, intro)
      const reporter = exploranda.Gopher(dependencies);
      reporter.report((e, n) => callback(e, intro, {stage: vars, results: n}));
    }
    function performOutro(intro, main, callback) {
      const {vars, dependencies} = makeOutroDependencies(event, context, intro, main)
      const reporter = exploranda.Gopher(dependencies);
      reporter.report((e, n) => callback(e, intro, main, {stage: vars, results: n}));
    }
    function performCleanup(intro, main, outro, callback) {
      setTimeout(() => {
        try {
        callback(null, transformInput('cleanup', config, {event, context, intro, main, outro}))
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
