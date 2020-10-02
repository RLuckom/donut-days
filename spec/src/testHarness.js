const _ = require('lodash')
const rewire = require('rewire')
const main = rewire('../../index.js')

function makeExplorandaMock(validators) {
  let calls = 0
  const keys = ['intro', 'main', 'outro']
  const finishedSteps = []
  function reporter(validator) {
    return {
      report: (f) => setTimeout(f, 0)
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
      console.log(`deps: ${JSON.stringify(dependencies)} keys: ${JSON.stringify(_.keys(validator.dependencies))}`)
      validateDependencies(dependencies, validator.dependencies)
      return reporter(validator)
    },
    dataSources: {AWS : { lambda: {invoke: true }}},
    finishedSteps
  }
}

function validateDependencies(dependencies, depGraphSpec) {
  _.map(depGraphSpec, (v, k) => {
    console.log(`testing ${k}, function ${dependencies[k]}`)
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

function newTransformInput(original, validators) {
  return function(stage, stageConfig, processParams) {
    const result = original(stage, stageConfig, processParams)
    const stageValidators = _.get(validators, [stage, 'dependencyInput'])
    validateDependencies(result, stageValidators)
    expect(_.keys(result).length).toEqual(_.keys(stageValidators).length)
    return result
  }
}

function generateTests(suiteName, testObjects) {
  describe(suiteName, () => {
    _.map(testObjects, ({name, onComplete, validators, config, event, context, helperFunctions, dependencyHelpers}) => {
      it(name, (done) => {
        const originalTransformInput = main.__get__('transformInput')
        const explorandaMock = makeExplorandaMock(validators)
        const unsetExploranda = main.__set__('exploranda', explorandaMock)
        const unsetTransformInput = main.__set__('transformInput', newTransformInput(originalTransformInput, validators))
        main.createTask(config, helperFunctions || {}, dependencyHelpers || {})(event, context || {}, () => {
          (onComplete || _.noop)(explorandaMock.finishedSteps)
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
