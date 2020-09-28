const _ = require('lodash')
const rewire = require('rewire')
const main = rewire('../../index.js')

function makeExplorandaMock(expectations) {
  let calls = 0
  const keys = ['intro', 'main', 'outro']
  const finishedSteps = []
  function reporter(expectation) {
    return {
      report: (f) => setTimeout(f, 0)
    }
  }
  return {
    Gopher: (dependencies) => {
      const expectation = expectations[keys[calls]]
      if (process.env.DONUT_DAYS_DEBUG) {
        console.log(`Testing expectations for ${keys[calls]}`)
      }
      finishedSteps.push(calls)
      calls++
      validateDependencies(dependencies, expectation.dependencies)
      return reporter(expectation)
    },
    dataSources: {AWS : { lambda: {invoke: true }}},
    finishedSteps
  }
}

function validateDependencies(dependencies, depGraphSpec) {
  _.map(depGraphSpec, (v, k) => {
    const result = v(dependencies[k])
    if (process.env.DONUT_DAYS_DEBUG) {
      console.log(`Testing ${k}: ${JSON.stringify(dependencies[k])}. result: ${result}`)
    }
    expect(result).toBeTruthy()
  })
  expect(_.keys(depGraphSpec).length).toEqual(_.keys(dependencies).length)
}

function newTransformInput(original, validators) {
  return function(transformers, stage, config, source) {
    const result = original(transformers, stage, config, source)
    stageValidators = _.get(validators, [stage, 'dependencyInput'])
    validateDependencies(result, stageValidators)
    expect(_.keys(result).length).toEqual(_.keys(stageValidators).length)
    return result
  }
}

function generateTests(suiteName, testObjects) {
  describe(suiteName, () => {
    _.map(testObjects, ({name, onComplete, validators, config, event, context, helperFunctions, dependencyHelpers}) => {
      it(name, (done) => {
        console.log(name)
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
