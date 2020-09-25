// var so we can rewire it in tests
var exploranda = require('exploranda-core');
const async = require('async')
const _ = require('lodash');

const apiConfig = {
  region: process.env.AWS_REGION
}

function createTask(config, makeDependencies) {
  const makePreDependencies = function(event, context) {
    return {}
  }
  const makeOutroDependencies = function(event, context, intro, main) {
    return {}
  }
  return function(event, context, callback) {
    function performIntro(callback) {
      const dependencies = makePreDependencies(event, context)
      const reporter = exploranda.Gopher(dependencies);
      reporter.report((e, n) => callback(e, n));
    }
    function performMain(intro, callback) {
      const dependencies = makeDependencies(event, context, _.cloneDeep(intro))
      const reporter = exploranda.Gopher(dependencies);
      reporter.report((e, n) => callback(e, intro, n));
    }
    function performOutro(intro, main, callback) {
      const dependencies = makeOutroDependencies(event, context, _.cloneDeep(intro), main)
      const reporter = exploranda.Gopher(dependencies);
      reporter.report((e, n) => callback(e, intro, n));
    }

    async.waterfall([
      performIntro,
      performMain,
      performOutro
    ], callback)
  }
}

function eventMatches(bucket, key, match) {
  const bucketMatches = (_(match.buckets).map((bucketName) => bucket === bucketName).some()) || !match.buckets
  const suffixMatches = (_(match.suffixes).map((suffix) => _.endsWith(key, suffix)).some()) || !match.suffixes
  const prefixMatches = (_(match.prefixes).map((pref) => _.startsWith(key, pref)).some()) || !match.prefixes
  return bucketMatches && suffixMatches && prefixMatches
}

function dependencies(buckets, keys, eventTargets, mediaId) {
  const functionInvocations = _.flattenDeep(_.map(_.zip(buckets, keys), ([bucket, key]) => {
    return _(eventTargets).map((et) => {
      const matches = _(et.matches).map(_.curry(eventMatches, bucket, key)).some()
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
