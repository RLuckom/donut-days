// var so we can rewire it in tests
var exploranda = require('exploranda-core');
const async = require('async')
const _ = require('lodash');
const uuid = require('uuid')
const raphlogger = require('raphlogger')

const defaults = {
  MAX_RECURSION_DEPTH: 3,
  MAX_BOUNCE: 7,
  MAX_STRING_LENGTH: 400,
}

function defaultLogger(level, message) {
  if (process.env.DONUT_DAYS_DEBUG === "true" || level === 'ERROR' || level === "WARN") {
    console.log(`${level}\t${message}`)
  }
}

// If this signature changes, remember to update the test harness or tests will break.
function transformInput(stage, stageConfig, processParams, log) {
  log.trace({tags: ["MAKE_INPUT"], metadata: {stage, config: safeStringify(stageConfig)}})
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

function processParams(helperFunctions, input, requireValue, log, params) {
  const output = {}
  _.each(params, (v, k) => {
    output[k] = processParamValue(helperFunctions, input, requireValue, log, v)
    if (_.isNull(output[k]) || _.isUndefined(output[k]) && requireValue) {
      // coponent: donut-days, tags: ["NULL_PARAM", "REQUIRED_VALUE"]
      log.error({tags: ["NULL_PARAM", "REQUIRED_VALUE"], metadata: {parameter: k, schema: safeStringify(v)}})
    }
    // component: donut-days, tags: ["PARAMETER_VALUE"], metadata: { stageName: <>, subStage: <>, partName: <>}
    log.trace({tags: ["PARAMETER_VALUE"], metadata: {name: k, plan: safeStringify(v), input: safeStringify(input), value: safeStringify(output[k])}})
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

  if (_.isString(value) || _.isNumber(value) || _.isBoolean(value) || !value) {
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

function processParamValue(helperFunctions, input, requireValue, log, value) {
  const transformers = {...builtInTransformations, ...helperFunctions}
  if (!_.isUndefined(value.value) || _.isFunction(value)) {
    return !_.isUndefined(value.value) ? value.value : value
  } else if (value.explorandaParams) {
    return _.reduce(value.explorandaParams, (acc, v, k) => {
      acc[k] = processExplorandaParamValue(v, _.partial(processParamValue, helperFunctions, input, requireValue, log))
      return acc
    }, {})
  } else if (value.ref) {
    return _.get(input, value.ref)
  } else if (value.every) {
    return _(processParams(helperFunctions, input, requireValue, log, value.every)).values().every()
  } else if (value.not) {
    return !processParamValue(helperFunctions, input, requireValue, log, value.not)
  } else if (value.some) {
    return _(processParams(helperFunctions, input, requireValue, log, value.some)).values().some()
  } else if (value.or) {
    return _(value.or).map(_.partial(processParamValue, helperFunctions, input, requireValue, log)).find()
  } else if (value.all) {
    return processParams(helperFunctions, input, requireValue, log, value.all)
  } else if (value.helper) {
    const helper = _.isString(value.helper) ? _.get(transformers, value.helper) : value.helper
    if (!_.isFunction(helper)) {
      // component: donut-days, tags: ["MISSING_HELPER"], metadata: { stageName: <>, subStage: <>, partName: <>, helperName: <>}
      log.error({tags: ["MISSING_HELPER"], metadata: { name: value.helper}})
    } else {
      return helper(processParams(helperFunctions, input, requireValue, log, value.params), {processParamValue: _.partial(processParamValue, helperFunctions, input, requireValue, log)})
    }
  }
  return value
}

function safeStringify(o) {
  if (_.isFunction(o)) {
    return typeof o
  }
  if (_.isObjectLike(o)) {
    const originalBufferJson = Buffer.prototype.toJSON
    Buffer.prototype.toJSON = function() { return this }
    let res
    try {
      res = JSON.stringify(o, (k, v) => {
        if (Buffer.isBuffer(v)) {
          return `Buffer: ${_.truncate(v.toString('base64'), {length: defaults.MAX_STRING_LENGTH })}`
        } else if (_.isString(v)) {
          return _.truncate(v, {length: defaults.MAX_STRING_LENGTH })
        } else {
          return v
        }
      })
      return res
    } catch(e) {
      Buffer.prototype.toJSON = originalBufferJson
      throw e
    }
  } else {
    return o
  }
}

function dependencyBuilders(helpers, log) { 
  return {
    ...{
      invokeFunction: (params, addDependency, addResourceReference, getDependencyName, processParams, processParamValue) => {
        const bounceDepth = (processParamValue({ref: 'event.bounceDepth'}) || 1) + 1
        const allowedBounceDepth = processParamValue({ref: 'overrides.MAX_BOUNCE'}) || defaults.MAX_BOUNCE
        if (allowedBounceDepth > bounceDepth) {
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
        } else {
          // component: donut-days, tags: ["DEPTH_LIMIT_EXCEEDED"]
          log.error({tags: ["DEPTH_LIMIT_EXCEEDED"], metadata: {type: 'bounce', depth: bounceDepth, allowed: allowedBounceDepth, params: safeStringify(params)}})
        }
      },
      genericApi: (params, addDependency) => {
        const url = params.url || params.uri
        let dep;
        if (_.isString(url)) {
          dep = {
            accessSchema: {
              name: 'GET url',
              dataSource: 'GENERIC_API',
            },
            params: {
              apiConfig: {value: {
                url: url
              }},
            }
          }
        } else if (_.isArray(url) && url.length > 0 ) {
          dep = {
            accessSchema: {
              name: 'GET url',
              dataSource: 'GENERIC_API',
              mergeIndividual: _.get(params, 'mergeIndividual')
            },
            params: {
              apiConfig: {value: _.map(url, (url) => {
                return {url}
              })}
            }
          }
        } else if (params.apiConfig && !(_.isArray(params.apiConfig) && params.apiConfig.length < 1)) {
          dep = {
            accessSchema: {
              name: 'GET url',
              dataSource: 'GENERIC_API',
            },
            params: {
              apiConfig: { value: params.apiConfig }
            }
          }
        }
        if (dep && params.allow404) {
          _.set(dep, 'accessSchema.onError', (err, res) => {
            if (err && res.statusCode === 404) {
              return {res: 404}
            }
            return {err, res}
          })
        }
        addDependency(null, dep)
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
          // component: donut-days, tags: ["DEPTH_LIMIT_EXCEEDED"]
          log.error({tags: ["DEPTH_LIMIT_EXCEEDED"], metadata: {type: 'recursion', depth: recursionDepth, allowed: allowedRecursionDepth, params: safeStringify(params)}})
        }
      },
      eventConfiguredDD: (params, addDependency, addResourceReference, getDependencyName, processParams, processParamValue, addFullfilledResource) => {
        const bounceDepth = (processParamValue({ref: 'event.bounceDepth'}) || 1) + 1
        const allowedBounceDepth = processParamValue({ref: 'overrides.MAX_BOUNCE'}) || defaults.MAX_BOUNCE
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
        if (allowedBounceDepth > bounceDepth) {
        addDependency(null,  {
          accessSchema: exploranda.dataSources.AWS.lambda.invoke,
          params: {
            FunctionName: {
              value: params.FunctionName
            },
            InvocationType: {value: params.InvocationType || 'Event'},
            Payload: {
              value: JSON.stringify({
                event: {...params.event, ...{bounceDepth}},
                config: {...params.config, ...{expectations}}
              })
            }
          }
        })
        } else {
          // component: donut-days, tags: ["DEPTH_LIMIT_EXCEEDED"]
          log.error({tags: ["DEPTH_LIMIT_EXCEEDED"], metadata: {type: 'bounce', depth: bounceDepth, allowed: allowedBounceDepth, params: safeStringify(params)}})
        }
      },
      DD: (params, addDependency, addResourceReference, getDependencyName, processParams, processParamValue, addFullfilledResource) => {
        const bounceDepth = (processParamValue({ref: 'event.bounceDepth'}) || 1) + 1
        const allowedBounceDepth = processParamValue({ref: 'overrides.MAX_BOUNCE'}) || defaults.MAX_BOUNCE
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
        if (allowedBounceDepth > bounceDepth) {
          addDependency(null,  {
            accessSchema: exploranda.dataSources.AWS.lambda.invoke,
            params: {
              FunctionName: {
                value: params.FunctionName
              },
              InvocationType: {value: params.InvocationType || 'Event'},
              Payload: {
                value: JSON.stringify({
                  event: {...params.event, ...{bounceDepth}},
                  expectations,
                }
                                     )
              }
            }
          })
        } else {
          // component: donut-days, tags: ["DEPTH_LIMIT_EXCEEDED"]
          log.error({tags: ["DEPTH_LIMIT_EXCEEDED"], metadata: {type: 'bounce', depth: bounceDepth, allowed: allowedBounceDepth, params: safeStringify(params)}})
        }
      },
      explorandaUpdated: (params, addDependency, addResourceReference, getDependencyName, processParams, processParamValue) => {
        addDependency(params.dependencyName, {
          accessSchema: _.isString(params.accessSchema) ? _.get(exploranda, params.accessSchema) : params.accessSchema,
          params: params.params || (params.explorandaParams ? { explorandaParams: params.explorandaParams} : params.params),
          behaviors: params.behaviors,
        })
      },
      exploranda: (params, addDependency, addResourceReference, getDependencyName, processParams, processParamValue) => {
        addDependency(params.dependencyName, {
          accessSchema: _.isString(params.accessSchema) ? _.get(exploranda, params.accessSchema) : params.accessSchema,
          params: params.params || (params.explorandaParams ? processParamValue({ explorandaParams: params.explorandaParams}) : params.params),
          behaviors: params.behaviors,
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

function generateDependencies(input, config, transformers, mergedDependencyBuilders, log) {
  const dependencies = {}
  const dependencyToConfigStepMap = {}
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
      // component: donut-days, tags: ["DRY_RUN_DEPENDENCY"]
      log.info({tags: ["DRY_RUN_DEPENDENCY"], metadata: {name, config: safeStringify(stringableDependency(dep))}})
    } else {
      dependencies[name] = dep
      if (!dependencyToConfigStepMap[prefix]) {
        dependencyToConfigStepMap[prefix] = []
      }
      dependencyToConfigStepMap[prefix].push(name)
    }
    return name
  }
  _.each(config, (desc, name) => {
    if (testEvent(name, desc.condition, _.partial(processParamValue, transformers, input, false, log), log)) {
      builder = _.get(mergedDependencyBuilders, desc.action)
      return builder(
        processParams(transformers, input, false, log, desc.params),
        _.partial(addDependency, desc.dryRun, name),
        _.partial(addResourceReference, name, _.partial(processParams, transformers, input, false, log)),
        _.partial(getQualifiedName, name),
        _.partial(processParams, transformers, input, false, log),
        _.partial(processParamValue, transformers, input, false, log),
        addFullfilledResource,
        transformers
      )
    }
  })
  const formatters = _.map(dependencyToConfigStepMap, (addedDepNames, configStepName) => {
    return function(dependencyResults) {
      const formatter = _.get(config, [configStepName, 'formatter'])
      if (_.isFunction(formatter)) {
        dependencyResults[configStepName] = formatter(_.pick(dependencyResults, addedDepNames))
      }
      else if (formatter) {
        const resolvedFormatter = processParamValue(transformers, input, false, log, formatter)
        if (_.isFunction(resolvedFormatter)) {
          dependencyResults[configStepName] = resolvedFormatter(_.pick(dependencyResults, addedDepNames))
        }
      }
      return dependencyResults
    }
  })
  return {dependencies, resourceReferences, fulfilledResources, formatters}
}

function stringableDependency(dep) {
  const stringable = _.cloneDeep(dep)
  stringable.accessSchema = {dataSource: _.get(dep, 'accessSchema.dataSource'), name: _.get(dep, 'accessSchema.name')}
  return stringable
}

const testEvent = function(name, condition, processParamValue, log) {
  const notApplicable = !condition
  let processed = null
  if (condition) {
    processed = processParamValue(condition)
  }
  const result = notApplicable || processed
  // component: donut-days, tags: ["CONDITION_TEST"], metadata: { condition, notApplicable, processResult, overallResult }
  log.trace({tags: ["CONDITION_TEST"], metadata: { name, condition: condition ? safeStringify(condition) : condition, notApplicable, processResult: _.isObjectLike(processed) ? safeStringify(processed) : processed, overallResult: result }})
  return result
}

function logStage(stage, vars, dependencies, resourceReferences, fulfilledResources, log) {
  // component: donut-days, tags: ["STAGE_SUMMARY"], metadata: { vars, results, resourceReferences, fulfilledResources }
  const deps = safeStringify(_.reduce(dependencies, (acc, v, k) => {
    acc[k] = stringableDependency(v)
    return acc
  }, {}))
  log.trace({tags: ["STAGE_SUMMARY"], metadata: {stage, vars:_.isObjectLike(vars) ? safeStringify(vars) : vars,  deps, resourceReferences: safeStringify(resourceReferences), fulfilledResources: safeStringify(fulfilledResources)}})
}

// If this signature changes, remember to update the test harness or tests will break.
function createTask(config, helperFunctions, dependencyHelpers, recordCollectors, logger) {
  const log = raphlogger.init(logger, { component: 'donut-days' })
  log.trace({tags: ["CREATE_TASK"]})
  const expectations = _.cloneDeep(_.get(config, 'expectations') || {})
  const condition = _.cloneDeep(_.get(config, 'condition'))
  const cleanup = _.cloneDeep(_.get(config, 'cleanup') || {})
  const overrides = _.cloneDeep(_.get(config, 'overrides') || {})
  const stages = _.cloneDeep(_.get(config, 'stages' || {}))
  // TODO document why the expectations key is weird
  function addRecordCollectors(gopher) {
    _.each(recordCollectors, (v, k) => {
      gopher.recordCollectors[k] = v
    })
  }
  const mergedDependencyBuilders = dependencyBuilders(dependencyHelpers, log)
  function makeStageDependencies(stageName, context) {
    const stageConfig = _.get(stages, [stageName, 'transformers'])
    log.trace({tags: ["MAKE_STAGE_DEPENDENCIES"], metadata: {stageName}})
    const stageEnabled = testEvent(stageName, _.get(stages, [stageName, 'condition']), _.partial(processParamValue, helperFunctions, {...context}, false, log), log)
    const input = stageEnabled ? transformInput(stageName, stageConfig, _.partial(processParams, helperFunctions, context, false, log), log) : {}
    const stageDependencies = stageEnabled ? generateDependencies({...{stage: input}, ...context}, _.get(stages, [stageName, 'dependencies']), helperFunctions, mergedDependencyBuilders, log) : {}
    return {...{vars: input}, ...stageDependencies }
  }
  return function(event, context, callback) {
    log.setSource(_.get(context, 'invokedFunctionArn'))
    log.setSourceInstance(_.get(context, 'awsRequestId'))
    // component: donut-days, tags: ["EVENT_RECEIPT"], metadata: {functionName, event}
    log.info({tags: ["EVENT_RECEIPT"], metadata: { event: safeStringify(event)}})
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
        const {vars, dependencies, resourceReferences, formatters, fulfilledResources} = makeStageDependencies(stageName, stageContext)
        markExpectationsFulfilled(fulfilledResources)
        logStage(stageName, vars, dependencies, resourceReferences, fulfilledResources, log)
        const reporter = exploranda.Gopher(dependencies);
        addRecordCollectors(reporter)
        reporter.report((e, n, metrics) => {
          if (n) {
            _.each(formatters, (f) => f(n))
          }
          callback(e, { [stageName] : {vars, resourceReferences, metrics, results: n}, ...stageContext});
        })
      }
    }
    function checkExpectationsFulfilled() {
      // component: donut-days, tags: ["EVENT_RECEIPT"], metadata: {functionName, event}
      log.trace({tags: ["CHECK_EXPECTATIONS"], metadata: { expectations: safeStringify(expectations)} })
      unfulfilledExpectations = _.reduce(expectations, (acc, v, k) => {
        if (!v.fulfilled) {
          acc[k] = v
        }
        return acc;
      }, {})
      if (_.values(unfulfilledExpectations).length) {
        // component: donut-days, tags: ['UNFULFILLED_EXPECTATION'] metadata: { unfulfilledExpectations, errorMessage}
        const msg = `[ Unfulfilled expectations: ${safeStringify(unfulfilledExpectations)} ] [ error: ${errorOnUnfulfilledExpectation} ]`
        log.error({tags:  ["UNFULFILLED_EXPECTATIONS"], metadata: {Unfulfilled: safeStringify(unfulfilledExpectations), error: errorOnUnfulfilledExpectation}})
        if (errorOnUnfulfilledExpectation) {
          throw new Error(msg)
        }
      }
    }
    function performCleanup(...args) {
      const runContext = {...(args.length === 2 ? args[0] : {}), ...{event, context, config}}
      const callback = args[1] || args[0]
      const metrics = _.reduce(runContext, (acc, v, k) => {
        const metrics = _.get(v, 'metrics')
        if (metrics) {
          acc[k] = metrics
        }
        return acc
      }, {})
      setTimeout(() => {
        log.trace({tags: ["CLEANUP"]})
        try {
          checkExpectationsFulfilled()
          const stageConfig = _.get(cleanup, 'transformers')
          callback(null, transformInput('cleanup', stageConfig, _.partial(processParams, helperFunctions, runContext, false, log), log), metrics)
        } catch(e) {
          callback(e)
        }
      }, 0)
    }
    if (testEvent('task', condition, _.partial(processParamValue, helperFunctions, {event, context}, false, log), log)) {
      // component: donut-days, tags: ['EVENT_MATCH']
      log.debug({tags: ["EVENT_MATCH"]})
      const stageFunctions = _(stages).toPairs().sortBy(([name, conf]) => conf.index).map(([name, conf], index) => {
        if (conf.index !== index) {
          // component: donut-days, tags: ['REORDERED_STAGE'] metadata: { statedIndex, insertedIndex} 
          log.warn({tags: ['REORDERED_STAGE'], metadata: { statedIndex: conf.index, insertedIndex: index}})
        }
        return stageExecutor(name)
      }).value()
      async.waterfall(_.concat(stageFunctions, [performCleanup]), callback)
    } else {
      // component: donut-days, tags: ['EVENT_NON_MATCH']
      log.debug({tags: ['EVENT_NON_MATCH']})
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
