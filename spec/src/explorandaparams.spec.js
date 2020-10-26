const _ = require('lodash')
const {generateTests} = require('./testHarness')

const exp1 = {
  name: 'exp1',
  validators: {
    intro: {
      dependencies: {
      },
      dependencyInput: {
        params: ({string, eventParam, sourceOnly, simpleConstructedSource, complexConstructedSource, arrayConstructedSource, sourceAndFormatter, sourceAndFormatterFunction, sourceAndFormatterFunctionAs, sourceAndFormatterFunctionArrayLike}) => {
          const dep4 = sourceAndFormatterFunction.formatter({dep4: ['f00']})
          const dep40 = sourceAndFormatterFunctionArrayLike.formatter({dep4: ['f00']})
          const dep400 = sourceAndFormatter.formatter({dep4: ['dep4']})
          const dep4000 = sourceAndFormatterFunctionAs.formatter({dep4: ['f00']})
          return (
            _.isEqual(string, {value: 'string param' })
            && _.isEqual(eventParam, {value: 'bar'})
            && _.isEqual(sourceOnly, {source: 'dep0'})
            && _.isEqual(simpleConstructedSource, {source: 'dep1'})
            && _.isEqual(complexConstructedSource, {source: 'dep2_step1'})
            && _.isEqual(complexConstructedSource, {source: 'dep2_step1'})
            && _.isEqual(arrayConstructedSource, {source: ['dep3_step2', 'dep4']})
            && _.isEqual(sourceAndFormatter.source, 'dep4')
            && _.isEqual(dep400, 'dep4')
            && _.isEqual(sourceAndFormatter.source, 'dep4')
            && _.isEqual(dep4, 'f00')
            && _.isEqual(dep4000, 'f00')
            && _.isEqual(dep40, ['f00'])
          )
        }
      }
    },
    main: {
      dependencies: {
      },
      dependencyInput: {}
    },
    outro: {
      dependencies: {
      },
      dependencyInput: {
      }
    }
  },
  config: {
    intro: {
      transformers: {
        params: {
          explorandaParams: {
            string: 'string param',
            eventParam: {ref: 'event.foo'},
            sourceOnly: { source: 'dep0'},
            simpleConstructedSource: { source: { configStepName: 'dep1'}},
            complexConstructedSource: { source: { configStepName: 'dep2', dependencyName: 'step1'}},
            arrayConstructedSource: { source: [{ configStepName: 'dep3', dependencyName: 'step2'}, 'dep4']},
            sourceAndFormatter: { 
              source: { configStepName: 'dep4'},
              formatter: 'dep4',
            },
            sourceAndFormatterFunction: { 
              source: { configStepName: 'dep4'},
              formatter:  ({dep4}) => dep4,
            },
            sourceAndFormatterFunctionAs: { 
              source: { configStepName: 'dep4', as: 'foo'},
              formatter:  ({foo}) => foo,
            },
            sourceAndFormatterFunctionArrayLike: { 
              source: { configStepName: 'dep4', isArrayLike: true },
              formatter:  ({dep4}) => dep4,
            },
          }
        }
      },
    },
  },
  event: {
    foo: 'bar'
  },
}

generateTests('explorandaParams', [exp1])
