const _ = require('lodash')
const {generateTests} = require('./index.spec')

const test1 = {
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
    matches: {
      'foo.bar': 4
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
    matches: {
      'foo.bar': 4
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

generateTests('Basic', [test1, test2, test3, test4])
