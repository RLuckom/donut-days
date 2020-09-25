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
    finishedSteps
  }
}

function validateDependencies(dependencies, depGraphSpec) {
  _.map(depGraphSpec, (v, k) => {
    const result = v(dependencies[k])
    if (process.env.DONUT_DAYS_DEBUG) {
      console.log(`Testing ${k}: ${dependencies[k]}. result: ${result}`)
    }
    expect(result).toBeTruthy()
  })
}

function generateTests(suiteName, testObjects) {
  describe(suiteName, () => {
    _.map(testObjects, ({name, onComplete, validators, config, event, context, makeDependencies}) => {
      it(name, (done) => {
        const explorandaMock = makeExplorandaMock(validators)
        main.__set__('exploranda', explorandaMock)
        main.createTask(config, makeDependencies)(event, context || {}, () => {
          (onComplete || _.noop)(explorandaMock.finishedSteps) 
          done()
        })
      })
    })
  })
}

module.exports = {
  generateTests
}
