// var so we can rewire it in tests
var exploranda = require('exploranda-core');
const async = require('async')
const _ = require('lodash');

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

const stageTransformers = {
  copy: (copyConfig, source, dest) => _.each(copyConfig, (v, k) => {
    trace(`${v}, ${k}, ${JSON.stringify(source)}, ${JSON.stringify(dest)}`)
    _.set(dest, v, _.get(source, k))
  })
}

function testCondition(condition, input) {
  return _(conditionTesters).map((v, k) => {
    const relevantCondition = _.get(condition, k)
    result = v(relevantCondition, input)
    trace(`[ condition: ${JSON.stringify(condition)} ] [ part: ${k} ] [ relevant: ${!!relevantCondition} ] [ result: ${!relevantCondition || result} ]`)
    return (!relevantCondition || result)
  }).every()
}

function transformStage(transformationConfig, source, dest) {
  return _(stageTransformers).each((v, k) => {
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

function populateDependencies(transformationConfig, source, dest) {
  return _(stageTransformers).each((v, k) => {
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

function transformInput(stage, config, source) {
  const stageConfig = _.get(config, [stage, 'transformers'])
  trace(`making input for ${stage} with ${JSON.stringify(stage)}`)
  const newInput = {}
  _.each(stageConfig, (v, k) => {
    trace(`transforming ${k} with config ${JSON.stringify(v)}`) 
    transformStage(v, source, newInput)
  })
  trace(`input for ${stage}: ${JSON.stringify(newInput)}`)
  return newInput
}

function populateDependencies(stage, config, source) {
  const stageConfig = _.get(config, [stage, 'dependenciess'])
  trace(`making input for ${stage} with ${JSON.stringify(stage)}`)
  const dependencies = {}
  _.each(stageConfig, (v, k) => {
    trace(`populating ${k} with config ${JSON.stringify(v)}`) 
    populateDependencies(v, source, newInput)
  })
  trace(`dependencies for ${stage}: ${JSON.stringify(dependencies)}`)
  return dependencies
}

function createTask(config, makeDependencies) {
  const testEvent = function(input) {
    return !config.conditions || _(config.conditions).map((v, k) => {
      trace(`Testing conditionSet ${k}`)
      const result = testConditionSet(k, v, input)
      trace(`Tested conditionSet ${k}. result: ${result}`)
      return result
    }).some()
  }
  const makeIntroDependencies = function(event, context) {
    trace('intro')
    const input = transformInput('intro', config, {event, context})
    return {}
  }
  const makeMainDependencies = function(event, context, intro) {
    const input = transformInput('main', config, {event, context, intro})
    return makeDependencies(input)
  }
  const makeOutroDependencies = function(event, context, intro, main) {
    return {}
  }
  return function(event, context, callback) {
    function performIntro(callback) {
      const dependencies = makeIntroDependencies(event, context)
      trace(JSON.stringify(dependencies))
      const reporter = exploranda.Gopher(dependencies);
      reporter.report((e, n) => callback(e, n));
    }
    function performMain(intro, callback) {
      const dependencies = makeMainDependencies(event, context, _.cloneDeep(intro))
      const reporter = exploranda.Gopher(dependencies);
      reporter.report((e, n) => callback(e, intro, n));
    }
    function performOutro(intro, main, callback) {
      const dependencies = makeOutroDependencies(event, context, _.cloneDeep(intro), main)
      const reporter = exploranda.Gopher(dependencies);
      reporter.report((e, n) => callback(e, intro, n));
    }
    if (testEvent({event, context})) {
      debug(`event ${event ? JSON.stringify(event) : event} matched for processing`)
      async.waterfall([
        performIntro,
        performMain,
        performOutro
      ], callback)
    } else {
      debug(`event ${event ? JSON.stringify(event) : event} did not match for processing`)
      callback()
    }
  }
}

function dependencies(buckets, keys, eventTargets, mediaId) {
  const functionInvocations = _.flattenDeep(_.map(_.zip(buckets, keys), ([bucket, key]) => {
    return _(eventTargets).map((et) => {
      const matches = _(et.matches).map(_.partial(eventMatches, bucket, key)).some()
      if (matches) {
        return JSON.parse(
          JSON.stringify(et.functionInvocations)
          .replace(/\$bucket/g, bucket)
          .replace(/\$key/g, key)
          .replace(/\$mediaId/g, mediaId)
        )
      }
  }).filter().value()
  }))
  return {
    invokeFunctions: {
      accessSchema: exploranda.dataSources.AWS.lambda.invoke,
      params: {
        FunctionName: {
          value: _.map(functionInvocations, 'functionArn')
        },
        InvocationType: {value: 'Event'},
        Payload: {
          value: _.map(functionInvocations, (fi) => JSON.stringify(fi.eventSchema))
        }
      }
    },
  }
}

const handler = function(event, context, callback) {
  const eventTargets = JSON.parse(process.env.MEDIA_EVENT_TARGETS)
  const keys = _.map(event.Records, 's3.object.key')
  const buckets = _.map(event.Records, 's3.bucket.name')
  const mediaId = uuid.v4()
  const reporter = exploranda.Gopher(dependencies(buckets, keys, eventTargets, mediaId));
  reporter.report((e, n) => callback(e, n));
}

module.exports = {
  createTask
}
