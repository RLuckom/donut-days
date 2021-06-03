const _ = require('lodash')
const rewire = require('rewire')
const main = rewire('../../index.js')

function makeExplorandaMock(validators, {err, results, metrics}, config) {
  let calls = 0
  const newConfig = _.cloneDeep(config)
  const {conditions, expectations, cleanup, overrides, stages} = newConfig
  const keys = _(stages).toPairs().sort(([n, c]) => c.index).map(([n, c]) => n).value()

  const finishedSteps = []
  function reporter() {
    return {
      report: (f) => setTimeout(() => f(err, results, metrics), 0)
    }
  }
  return {
    Gopher: (dependencies) => {
      const validator = validators[keys[calls]]
      if (process.env.DONUT_DAYS_DEBUG) {
        console.log(`Testing validators for ${keys[calls]}`)
      }
      finishedSteps.push(calls)
      calls++
        if (process.env.DONUT_DAYS_DEBUG) {
        console.log(`deps: ${JSON.stringify(dependencies)} keys: ${JSON.stringify(_.keys(validator.dependencies))}`)
      }
      validateDependencies(dependencies, validator.dependencies)
      return reporter()
    },
    dataSources: {AWS : { lambda: {invoke: true }}},
    finishedSteps
  }
}

function validateDependencies(dependencies, depGraphSpec) {
  _.map(depGraphSpec, (v, k) => {
    if (process.env.DONUT_DAYS_DEBUG) {
      console.log(`testing ${k}: ${dependencies[k]}`)
    }
    const result = v(dependencies[k])
    if (process.env.DONUT_DAYS_DEBUG) {
      console.log(`Testing ${k}: ${JSON.stringify(dependencies[k])}. result: ${result}`)
    }
    expect(result).toBeTruthy()
  })
  if (process.env.DONUT_DAYS_DEBUG) {
    console.log(`Expecting ${JSON.stringify(dependencies)} to have the keys: ${JSON.stringify(_.keys(depGraphSpec))}`)
  }
  expect(_.keys(depGraphSpec).length).toEqual(_.keys(dependencies).length)
}

function validateResources(resourceReferences, resourceSpec) {
  _.map(resourceSpec, (v, k) => {
    if (process.env.DONUT_DAYS_DEBUG) {
      console.log(`testing resource ${k}: ${resourceReferences[k]}`)
    }
    const result = v(resourceReferences[k])
    if (process.env.DONUT_DAYS_DEBUG) {
      console.log(`Testing resource ${k}: ${JSON.stringify(resourceReferences[k])}. result: ${result}`)
    }
    expect(result).toBeTruthy()
  })
  if (process.env.DONUT_DAYS_DEBUG) {
    console.log(`Expecting ${JSON.stringify(resourceReferences)} to have the keys: ${JSON.stringify(_.keys(resourceSpec))}`)
  }
  expect(_.keys(resourceSpec).length).toEqual(_.keys(resourceReferences).length)
}

function newTransformInput(original, validators) {
  return function(stage, stageConfig, processParams, log) {
    const result = original(stage, stageConfig, processParams, log)
    const stageValidators = _.get(validators, [stage, 'dependencyInput'])
    validateDependencies(result, stageValidators)
    expect(_.keys(result).length).toEqual(_.keys(stageValidators).length)
    return result
  }
}

function generateTests(suiteName, testObjects) {
  describe(suiteName, () => {
    _.map(testObjects, ({expectError, name, onComplete, validators, mockResults, config, event, context, helperFunctions, dependencyHelpers, output}) => {
      it(name, (done) => {
        const originalTransformInput = main.__get__('transformInput')
        const explorandaMock = makeExplorandaMock(validators, mockResults || {}, config)
        const unsetExploranda = main.__set__('exploranda', explorandaMock)
        const unsetTransformInput = main.__set__('transformInput', newTransformInput(originalTransformInput, validators))
        main.createTask(config, helperFunctions || {}, dependencyHelpers || {}, {}, _.noop)(event, context || {}, (e, r, metrics) => {
          (onComplete || _.noop)(explorandaMock.finishedSteps)
          console.log(metrics)
          if (output || (r && !_.isEqual({}, r))) {
            expect(output).toEqual(r)
          }
          if (expectError) {
            expect(e).toBeTruthy()
          } else {
            expect(e).toBeFalsy()
          }
          unsetExploranda()
          unsetTransformInput()
          done()
        })
      })
    })
  })
}

module.exports = {
  generateTests
}
