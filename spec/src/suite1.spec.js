const _ = require('lodash')
const {generateTests} = require('./testHarness')

const test1 = {
  name: 'nothing',
  validators: {
    intro: {
      dependencies: {
      }
    },
    main: {
      dependencies: {
      }
    },
    outro: {
      dependencies: {
      }
    }
  },
  config: {},
  event: {},
  makeDependencies: () => {
    return {}
  }
}

const test2 = {
  name: 'main string dep',
  validators: {
    intro: {
      dependencies: {
      }
    },
    main: {
      dependencies: {
        one: _.isString
      }
    },
    outro: {
      dependencies: {
      }
    }
  },
  config: {
  },
  event: {},
  makeDependencies: () => {
   return { one : 'one' }
  } 
}

const test3 = {
  name: 'condition matches',
  validators: {
    intro: {
      dependencies: {
      }
    },
    main: {
      dependencies: {
        one: _.isString
      }
    },
    outro: {
      dependencies: {
      }
    }
  },
  config: {
    conditions: {
      doesMatch: [{
        matchesAll: {
          'event.foo.bar': 4
        }
      }],
      doesNotMatch: [{
        matchesAll: {
          'event.foo.bar': 7
        }
      }]
    }
  },
  event: {
    foo: {
      bar: 4
    }
  },
  onComplete: (finishedSteps) => expect(finishedSteps.length).toEqual(3),
  makeDependencies: () => {
   return { one : 'one' }
  } 
}

const test4 = {
  name: 'condition matches all',
  validators: {
    intro: {
      dependencies: {
      }
    },
    main: {
      dependencies: {
        one: _.isString
      }
    },
    outro: {
      dependencies: {
      }
    }
  },
  config: {
    conditions: {
      doesNotMatch: [{
        matchesAll: {
          'event.foo.bar': 4
        }
      }]
    }
  },
  event: {
    foo: {
      bar: 7
    }
  },
  onComplete: (finishedSteps) => expect(finishedSteps.length).toEqual(0),
  makeDependencies: () => {
   return { one : 'one' }
  } 
}

const test5 = {
  name: 'condition matches 2',
  validators: {
    intro: {
      dependencyInput: {
        one: (n) => n === 4
      },
      dependencies: {
      }
    },
    main: {
      dependencies: {
        one: _.isString
      }
    },
    outro: {
      dependencies: {
      }
    }
  },
  config: {
    conditions: {
      doesMatchCopy: [{
        matchesAll: {
          'event.foo.bar': 4
        }
      }]
    },
    intro: {
      transformers: {
        copyFooBar: [{
          copy: {
            'event.foo.bar': 'one'
          }
        }]
      },
    }
  },
  event: {
    foo: {
      bar: 4
    }
  },
  onComplete: (finishedSteps) => expect(finishedSteps.length).toEqual(3),
  makeDependencies: () => {
   return { one : 'one' }
  } 
}

const test6 = {
  name: 'test6',
  validators: {
    intro: {
      dependencyInput: {
        one: (n) => n === 4
      },
      dependencies: {
      }
    },
    main: {
      dependencies: {
        one: _.isString
      }
    },
    outro: {
      dependencyInput: {
        one: (n) => n === 4
      },
      dependencies: {
        nextFunction_invoke: (dep) => {
          console.log(JSON.stringify(dep))
          return (dep.accessSchema && dep.params.FunctionName.value === 4
                  && dep.params.Payload.value === 4)
        }
      }
    }
  },
  config: {
    conditions: {
      doesMatchCopy: [{
        matchesAll: {
          'event.foo.bar': 4
        }
      }]
    },
    intro: {
      transformers: {
        copyFooBar: [{
          copy: {
            'event.foo.bar': 'one'
          }
        }]
      },
    },
    outro: {
      transformers: {
        copyFooBar: [{
          copy: {
            'event.foo.bar': 'one'
          }
        }]
      },
      dependencies: {
        nextFunction: {
          action: 'invokeFunction',
          params: {
            FunctionName: { ref: 'one' },
            Payload: { ref: 'one' }
          }
        }
      }
    },
  },
  event: {
    foo: {
      bar: 4
    }
  },
  onComplete: (finishedSteps) => expect(finishedSteps.length).toEqual(3),
  makeDependencies: () => {
   return { one : 'one' }
  } 
}

generateTests('Basic', [test1, test2, test3, test4, test5, test6])
