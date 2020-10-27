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
  qualifiedDependencyName: ({configStepName, dependencyName}) => getQualifiedName(configStepName, dependencyName),
  template: ({templateString, templateArguments}) => _.template(templateString)(templateArguments),
  mapTemplate: ({templateString, templateArgumentsArray}, {processParamValue}) => _.map(templateArgumentsArray, (templateArguments) => _.template(templateString)(processParamValue(templateArguments))),
  isInList: ({item, list}) => list.indexOf(item) !== -1,
  msTimestamp: () => {
    return new Date().getTime()
  },
  verifySlackSignature: ({credentials, messageSig, messageBody, timestampEpochSeconds}) => {
    const [v, sig] = messageSig.split('=')
    const hashInput = `${v}:${timestampEpochSeconds}:${messageBody}`
    const hmac = require('crypto').createHmac('sha256', credentials.signingSecret);
    hmac.update(hashInput)
    const digest = hmac.digest('hex')
    const hashValid = digest === sig
    const receiveTime = _.toInteger(new Date().getTime() / 1000)
    const sentTime = _.toInteger(timestampEpochSeconds)
    const ageSeconds = receiveTime - sentTime
    const timely = ageSeconds < 300
    return {
      result: hashValid && timely,
      hashValid,
      timely,
      details: {
        digest,
        sig,
        receiveTime,
        sentTime,
        ageSeconds,
      }
    }
  },
  bufferToString: ({buffer, encoding}) => {
    return buffer.toString(encoding)
  },
  map: ({list, handler}) => {
    return _.map(list, handler)
  },
  transform: ({arg, func}) => {
    return func(arg)
  },
}

function processParams(helperFunctions, input, requireValue, params) {
  const output = {}
  _.each(params, (v, k) => {
    output[k] = processParamValue(helperFunctions, input, requireValue, v)
    if (_.isNull(output[k]) || _.isUndefined(output[k]) && requireValue) {
      error(`parameter ${k} returned null or undefined value from schema ${safeStringify(v)} : ${output[k]}`)
    }
    trace(`Processed param [ name: ${k} ] [ plan: ${v ? JSON.stringify(v) : v} ] [ input: ${_.isObjectLike(input) ? JSON.stringify(input) : input} ] [ result: ${_.isObjectLike(output[k]) ? JSON.stringify(output[k]) : output[k]} ]`)
  })
  return output
}

function processExplorandaParamValue(value, processParamValue) {
  const source = _.get(value, 'source')
  const formatter = _.get(value, 'formatter')

  function sourceElementToString(sourceElement) {
    if (_.isString(sourceElement) || _.isInteger(sourceElement)) {
      return sourceElement
    } else if (sourceElement.configStepName || sourceElement.dependencyName) {
      return getQualifiedName(sourceElement.configStepName, sourceElement.dependencyName)
    } else {
      return processParamValue(sourceElement)
    }
  }

  function withNormalParams(source, f) {
    const sourceArray = _.isArray(source) ? _.cloneDeep(source) : [_.cloneDeep(source)]
    const sourceInstructions = _.zipObject(_.map(sourceArray, sourceElementToString), sourceArray)
    return function normalizer(params) {
      const normalizedParams = _.reduce(params, (acc, v, k) => {
        const instructions = sourceInstructions[k]
        const newKey = _.get(instructions, 'as') || k
        if (!_.isArray(v)) {
          acc[newKey] = v
        } else if (v.length !== 1) {
          acc[newKey] = v
        } else if (_.get(instructions, 'isArrayLike')) {
          acc[newKey] = v
        } else {
          acc[newKey] = v[0]
        }
        return acc
      }, {})
      return f(normalizedParams)
    }
  } 

  if (_.isString(value) || _.isNumber(value) || !value) {
    return { value }
  } else if (source || formatter) {
    const sourceConfig = {}
    if (source) {
      if (_.isString(source)) {
        sourceConfig.source = source
      } else if (_.isArray(source)) {
        sourceConfig.source = _.map(source, sourceElementToString) 
      } else if (source.configStepName) {
        sourceConfig.source = getQualifiedName(source.configStepName, source.dependencyName) 
      } else {
        sourceConfig.source = processParamValue(source)
      }
    }
    if (formatter) {
      const norm = _.partial(withNormalParams, source)
      if (_.isString(formatter) || _.isNumber(formatter) || _.isArray(formatter)) {
        sourceConfig.formatter = norm((params) => _.get(params, formatter))
      } else if (_.isFunction(formatter)) {
        const sourceArray = _.isArray(source) ? _.cloneDeep(source) : [_.cloneDeep(source)]
        const sourceInstructions = _.zipObject(_.map(sourceArray, sourceElementToString), sourceArray)
        function normalizer(params) {
          const normalizedParams = _.reduce(params, (acc, v, k) => {
             const instructions = sourceInstructions[k]
             const newKey = _.get(instructions, 'as') || k
             if (!_.isArray(v)) {
               acc[newKey] = v
             } else if (v.length !== 1) {
               acc[newKey] = v
             } else if (_.get(instructions, 'isArrayLike')) {
               acc[newKey] = v
             } else {
               acc[newKey] = v[0]
             }
             return acc
          }, {})
          return formatter(normalizedParams)
        }
        sourceConfig.formatter = norm(formatter)
      } else {
        const formatterValue = processParamValue(formatter)
        if (_.isString(formatterValue) || _.isNumber(formatterValue) || _.isArray(formatterValue)) {
          sourceConfig.formatter = norm((params) => _.get(params, formatterValue))
        }
      }
    }
    return sourceConfig
  } else {
    return {value: processParamValue(value)}
  }
}

function processParamValue(helperFunctions, input, requireValue, value) {
  const transformers = {...builtInTransformations, ...helperFunctions}
  if (value.value) {
    return value.value
  } else if (value.explorandaParams) {
    return _.reduce(value.explorandaParams, (acc, v, k) => {
      acc[k] = processExplorandaParamValue(v, _.partial(processParamValue, helperFunctions, input, requireValue))
      return acc
    }, {})
  } else if (value.ref) {
    return _.get(input, value.ref)
  } else if (value.every) {
    return _(processParams(helperFunctions, input, requireValue, value.every)).values().every()
  } else if (value.not) {
    return !processParamValue(helperFunctions, input, requireValue, value.not)
  } else if (value.some) {
    return _(processParams(helperFunctions, input, requireValue, value.some)).values().some()
  } else if (value.or) {
    return _(value.or).map(_.partial(processParamValue, helperFunctions, input, requireValue)).find()
  } else if (value.all) {
    return processParams(helperFunctions, input, requireValue, value.all)
  } else if (value.helper) {
    const helper = transformers[value.helper]
    if (!_.isFunction(helper)) {
      error(`No helper function named ${value.helper}. Instead found ${safeStringify(helper)}. Available helpers: ${JSON.stringify(_.keys(transformers))}`)
    } else {
      return transformers[value.helper](processParams(helperFunctions, input, requireValue, value.params), {processParamValue: _.partial(processParamValue, helperFunctions, input, requireValue)})
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
            InvocationType: {value: params.InvocationType || 'Event'},
            Payload: {
              value: params.Payload
            }
          }
        })
      },
      recurse: (params, addDependency, addResourceReference, getDependencyName, processParams, processParamValue) => {
        const recursionDepth = (processParamValue({ref: 'event.recursionDepth'}) || 1) + 1
        const allowedRecursionDepth = processParamValue({ref: 'overrides.MAX_RECURSION_DEPTH'}) || defaults.MAX_RECURSION_DEPTH
        if (allowedRecursionDepth > recursionDepth) {
          addDependency(null,  {
            accessSchema: exploranda.dataSources.AWS.lambda.invoke,
            params: {
              FunctionName: {
                value: processParamValue({ref: 'context.invokedFunctionArn'})
              },
              InvocationType: {value: params.InvocationType || 'Event'},
              Payload: {
                value: JSON.stringify({...params.Payload, ...{
                  recursionDepth: (processParamValue({ref: 'event.recursionDepth'}) || 1) + 1
                }})
            }}})
        } else {
          error(`Max recursion depth exceeded. [ depth: ${recursionDepth} ] [ allowedRecursionDepth: ${allowedRecursionDepth} ] [ params: ${safeStringify(params)} ]`)
        }
      },
      eventConfiguredDD: (params, addDependency, addResourceReference, getDependencyName, processParams, processParamValue, addFullfilledResource) => {
        const resourceReferences = processParams(params.resourceReferences)
        addResourceReference('resources', resourceReferences)
        const expectations = _.reduce(resourceReferences, (acc, ref, name) => {
          acc[name] = processParams({
            expectedResource: {value: ref },
            expectedBy: {
              all: {
                awsRequestId: {ref: 'context.awsRequestId' },
                functionName: {ref: 'context.functionName' }
              }
            }
          })
          return acc
        }, {})
        addDependency(null,  {
          accessSchema: exploranda.dataSources.AWS.lambda.invoke,
          params: {
            FunctionName: {
              value: params.FunctionName
            },
            InvocationType: {value: params.InvocationType || 'Event'},
            Payload: {
              value: JSON.stringify({
                event: params.event,
                config: {...params.config, ...{expectations}}
              })
            }
          }
        })
      },
      DD: (params, addDependency, addResourceReference, getDependencyName, processParams, processParamValue, addFullfilledResource) => {
        const resourceReferences = processParams(params.resourceReferences)
        addResourceReference('resources', resourceReferences)
        const expectations = _.reduce(resourceReferences, (acc, ref, name) => {
          acc[name] = processParams({
            expectedResource: {value: ref },
            expectedBy: {
              all: {
                awsRequestId: {ref: 'context.awsRequestId' },
                functionName: {ref: 'context.functionName' }
              }
            }
          })
          return acc
        }, {})
        addDependency(null,  {
          accessSchema: exploranda.dataSources.AWS.lambda.invoke,
          params: {
            FunctionName: {
              value: params.FunctionName
            },
            InvocationType: {value: params.InvocationType || 'Event'},
            Payload: {
              value: JSON.stringify({
                event: params.event,
                expectations,
                }
              )
            }
          }
        })
      },
      explorandaUpdated: (params, addDependency, addResourceReference, getDependencyName, processParams) => {
        addDependency(params.dependencyName, {
          accessSchema: _.isString(params.accessSchema) ? _.get(exploranda, params.accessSchema) : params.accessSchema,
          params: params.params
        })
      },
      exploranda: (params, addDependency, addResourceReference, getDependencyName, processParams) => {
        addDependency(params.dependencyName, {
          accessSchema: _.isString(params.accessSchema) ? _.get(exploranda, params.accessSchema) : params.accessSchema,
          params: processParams(params.params)
        })
      },
      explorandaDeprecated: (params, addDependency, addResourceReference, getDependencyName, processParams) => {
        addDependency(params.dependencyName, {
          accessSchema: _.isString(params.accessSchema) ? _.get(exploranda, params.accessSchema) : params.accessSchema,
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

function getQualifiedName(prefix, depName) {
  return depName ? `${prefix}_${depName}` : prefix
}


function generateDependencies(input, config, transformers, mergedDependencyBuilders) {
  const dependencies = {}
  const resourceReferences = {}
  const fulfilledResources = []
  function addFullfilledResource(ref) {
    fulfilledResources.push(ref)
  }
  function addResourceReference(prefix, processParams, refName,  ref) {
    const name = getQualifiedName(prefix, refName)
    resourceReferences[name] = ref
  }
  function addDependency(dryRun, prefix, depName, dep) {
    const name = getQualifiedName(prefix, depName)
    if (dryRun) {
      console.log(`Would add dependency ${name} consisting of ${JSON.stringify(stringableDependency(dep))}`)
    } else {
      dependencies[name] = dep
    }
    return name
  }
  _.each(config, (desc, name) => {
    if (testEvent(name, desc.conditions, _.partial(processParams, transformers, input, false))) {
      builder = mergedDependencyBuilders[desc.action]
      return builder(
        processParams(transformers, input, false, desc.params),
        _.partial(addDependency, desc.dryRun, name),
        _.partial(addResourceReference, name, _.partial(processParams, transformers, input)),
        _.partial(getQualifiedName, name),
        _.partial(processParams, transformers, input, false),
        _.partial(processParamValue, transformers, input, false),
        addFullfilledResource,
        transformers
      )
    }
  })
  return {dependencies, resourceReferences, fulfilledResources}
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

function logStage(stage, vars, dependencies, resourceReferences, fulfilledResources) {
  trace(`${stage}: [ vars: ${_.isObjectLike(vars) ? JSON.stringify(vars) : vars} ] [ deps: ${JSON.stringify(_.reduce(dependencies, (acc, v, k) => {
    acc[k] = stringableDependency(v)
    return acc
  }, {}))} ] [ resourceReferences: ${safeStringify(resourceReferences)} ] [ fulfilledResources: ${safeStringify(fulfilledResources)} ]`)
}

// If this signature changes, remember to update the test harness or tests will break.
function createTask(config, helperFunctions, dependencyHelpers, recordCollectors) {
  trace(`Building tasks with config: ${safeStringify(config)}`)
  const expectations = _.cloneDeep(_.get(config, 'expectations') || {})
  const conditions = _.cloneDeep(_.get(config, 'conditions') || {})
  const cleanup = _.cloneDeep(_.get(config, 'cleanup') || {})
  const overrides = _.cloneDeep(_.get(config, 'overrides') || {})
  const stages = _.cloneDeep(_.get(config, 'stages' || {}))
  // TODO document why the expectations key is weird
  function addRecordCollectors(gopher) {
    _.each(recordCollectors, (v, k) => {
      gopher.recordCollectors[k] = v
    })
  }
  const mergedDependencyBuilders = dependencyBuilders(dependencyHelpers)
  function makeStageDependencies(stageName, context) {
    const stageConfig = _.get(stages, [stageName, 'transformers'])
    trace(stageName)
    const input = transformInput(stageName, stageConfig, _.partial(processParams, helperFunctions, context, false))
    return {...{vars: input}, ...generateDependencies({...{stage: input}, ...context}, _.get(stages, [stageName, 'dependencies']), helperFunctions, mergedDependencyBuilders) }
  }
  return function(event, context, callback) {
    info(`event: ${safeStringify(event)}`)
    const errorOnUnfulfilledExpectation = _.isBoolean(_.get(config, 'errorOnUnfulfilledExpectation')) ? _.get(config, 'errorOnUnfulfilledExpectation') : true
    function markExpectationsFulfilled(fulfilledResources) {
      _.each(fulfilledResources, (r) => {
        _.each(expectations, (ex) => {
          if (_.isEqual(ex.expectedResource, r)) {
            ex.fulfilled = true
          }
        })
      })
    }
    function stageExecutor(stageName) {
      return function performStage(...args) {
        const stageContext = {...(args.length === 2 ? args[0] : {}), ...{event, context, config, overrides}}
        const callback = args[1] || args[0]
        const {vars, dependencies, resourceReferences, fulfilledResources} = makeStageDependencies(stageName, stageContext)
        markExpectationsFulfilled(fulfilledResources)
        logStage(stageName, vars, dependencies, resourceReferences, fulfilledResources)
        const reporter = exploranda.Gopher(dependencies);
        addRecordCollectors(reporter)
        reporter.report((e, n) => callback(e, { [stageName] : {vars, resourceReferences, results: n}, ...stageContext}));
      }
    }
    function checkExpectationsFulfilled() {
      trace(`[ all expectations: ${safeStringify(expectations)} ]`)
      unfulfilledExpectations = _.reduce(expectations, (acc, v, k) => {
        if (!v.fulfilled) {
          acc[k] = v
        }
        return acc;
      }, {})
      if (_.values(unfulfilledExpectations).length) {
        const msg = `[ Unfulfilled expectations: ${safeStringify(unfulfilledExpectations)} ] [ error: ${errorOnUnfulfilledExpectation} ]`
        error(msg)
        if (errorOnUnfulfilledExpectation) {
          throw new Error(msg)
        }
      }
    }
    function performCleanup(...args) {
      const runContext = {...(args.length === 2 ? args[0] : {}), ...{event, context, config}}
      const callback = args[1] || args[0]
      setTimeout(() => {
        trace('cleanup')
        try {
          checkExpectationsFulfilled()
          const stageConfig = _.get(cleanup, 'transformers')
          callback(null, transformInput('cleanup', stageConfig, _.partial(processParams, helperFunctions, runContext, false)))
        } catch(e) {
          callback(e)
        }
      }, 0)
    }
    if (testEvent('task', conditions, _.partial(processParams, helperFunctions, {event, context}, false))) {
      debug(`event ${event ? JSON.stringify(event) : event} matched for processing`)
      const stageFunctions = _(stages).toPairs().sortBy(([name, conf]) => conf.index).map(([name, conf], index) => {
        if (conf.index !== index) {
          warn(`stage ${name} has index ${conf.index} but is being inserted at ${index}`)
        }
        return stageExecutor(name)
      }).value()
      async.waterfall(_.concat(stageFunctions, [performCleanup]), callback)
    } else {
      debug(`event ${event ? JSON.stringify(event) : event} did not match for processing`)
      try {
        checkExpectationsFulfilled()
        callback()
      } catch(e) {
        callback(e)
      }
    }
  }
}

module.exports = {
  createTask,
  exploranda
}
