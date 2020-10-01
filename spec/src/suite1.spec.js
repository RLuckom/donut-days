const _ = require('lodash')
const {generateTests} = require('./testHarness')
const uuid = require('uuid')

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
}

const test2 = {
  name: 'test2',
  validators: {
    intro: {
      dependencyInput: {
        eight: (n) => n === 8,
        rest: (n) => _.isEqual(n, [2, 3, 4, 5]),
        all: (n) => _.isEqual(n, [1, 2, 3, 4, 5]),
        middle: (n) => _.isEqual(n, [2, 3, 4]),
        not: (n) => n === false,
      },
      dependencies: {
      }
    },
    main: {
      dependencies: {
        one_two: (n) => n === 'three',
      }
    },
    outro: {
      dependencies: {
      }
    }
  },
  config: {
    intro: {
      transformers: {
        not: {not: {value: 6} },
        eight: {or: [{ref: 'event.foo.bar'}, {value: 8}]},
        rest: {
          helper: "slice",
          params: {
            list: {ref: 'event.list'},
            start: {value: 1}
          }
        },
        all: {
          helper: "slice",
          params: {
            list: {ref: 'event.list'},
          }
        },
        middle: {
          helper: "slice",
          params: {
            list: {ref: 'event.list'},
            start: {value: 1},
            end: {value: 4},
          }
        },
      },
    },
    main: {
      dependencies: {
        one: {
          action: 'one'
        }
      }
    }
  },
  event: {
    list: [1, 2, 3, 4, 5],
  },
  dependencyHelpers: {
    one: (params, addDependency) => addDependency('two', 'three') 
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
}

const test4 = {
  name: 'condition matches all',
  validators: {
    intro: {
      dependencies: {
      }
    },
    main: {
    },
    outro: {
      dependencies: {
      }
    }
  },
  config: {
    conditions: {
      doesNotMatch: {
        helper: 'matches',
        params: {
          a: {ref: 'event.foo.bar'},
          b: {value: 4},
        }
      }
    },
    event: {
      foo: {
        bar: 7
      }
    },
    onComplete: (finishedSteps) => expect(finishedSteps.length).toEqual(0),
  }
}

const test5 = {
  name: 'test5',
  validators: {
    intro: {
      dependencyInput: {
        one: (n) => n === 4,
        mediaId: (m) => uuid.validate(m)
      },
    },
    main: {
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
      }],
    },
    intro: {
      transformers: {
        one: {ref: 'event.foo.bar'},
        mediaId: {helper: 'uuid'},
      },
      dependencies: {
      }
    }
  },
  event: {
    foo: {
      bar: 4
    }
  },
  onComplete: (finishedSteps) => expect(finishedSteps.length).toEqual(3),
}

const test6 = {
  name: 'test6',
  helperFunctions: {
    one: ({a, b}) => a + b
  },
  validators: {
    intro: {
      dependencyInput: {
        one: (n) => n === 4
      },
      dependencies: {
      }
    },
    main: {
    },
    outro: {
      dependencyInput: {
        one: (n) => n === 4
      },
      dependencies: {
        nextFunction_nextFunction: (dep) => {
          return (dep.accessSchema && dep.params.FunctionName.value === 4
                  && dep.params.Payload.value === 5)
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
        one: {ref: 'event.foo.bar'},
      },
    },
    outro: {
      transformers: {
        one: {ref: 'event.foo.bar'},
      },
      dependencies: {
        nextFunction: {
          conditions: {
            doesMatch: {
              helper: 'matches',
              params: {
                a: {ref: 'intro.vars.one'},
                b: {value: 4}
              }
            }
          },
          action: 'exploranda',
          params: {
            dependencyName: { value: 'nextFunction' },
            accessSchema: { value: 'dataSources.AWS.lambda.invoke'},
            params: {value: {
              FunctionName: {all: {
                value: { ref: 'stage.one'}
              }},
              Payload: {all: {
                value: { helper: 'one' ,
                  params: {
                    a: { ref: 'stage.one'},
                    b: { value: 1},
                  }
                }
              }
              }
            }
            }
          }
        },
        nextFunctionDryRun: {
          conditions: {
            doesMatch: {
              helper: 'matches',
              params: {
                a: {ref: 'intro.vars.one'},
                b: {value: 4}
              }
            }
          },
          action: 'exploranda',
          params: {
            dependencyName: { value: 'nextFunction' },
            dryRun: { value: true },
            accessSchema: { value: 'dataSources.AWS.lambda.invoke'},
            params: {value: {
              FunctionName: {all: {
                value: { ref: 'stage.one'}
              }},
              Payload: {all: {
                value: { helper: 'one' ,
                  params: {
                    a: { ref: 'stage.one'},
                    b: { value: 1},
                  }
                }
              }
              }
            }
            }
          }
        },
      },
    },
  },
  event: {
    foo: {
      bar: 4
    }
  },
  onComplete: (finishedSteps) => expect(finishedSteps.length).toEqual(3),
}

const test7 = {
  name: 'test7',
  helperFunctions: {
    one: ({a, b}) => a + b
  },
  validators: {
    intro: {
      dependencyInput: {
        one: (n) => n === 4
      },
      dependencies: {
      }
    },
    main: {
    },
    outro: {
      dependencyInput: {
        one: (n) => n === 4
      },
      dependencies: {
        nextFunctionEnabled_invoke: (dep) => {
          return (_.get(dep, 'accessSchema') && dep.params.FunctionName.value === 4
                  && dep.params.Payload.value === 5)
        }
      }
    }
  },
  config: {
    conditions: {
      doesMatchCopy: {
        helper: "matches",
        params: {
          a: {ref: "event.foo.bar"},
          b: {value: 4}
        }
      }
    },
    intro: {
      transformers: {
        one: {ref: 'event.foo.bar'},
      },
    },
    outro: {
      transformers: {
        one: {ref: 'event.foo.bar'},
      },
      dependencies: {
        nextFunctionEnabled: {
          conditions: {
            doesMatch: {
              some: {
                doesMatch: {
                  helper: "matches",
                  params: {
                    a: {ref: "event.foo.bar"},
                    b: {value: 4}
                  }
                },
                doesNotMatch: {
                  helper: "matches",
                  params: {
                    a: {ref: "event.foo.bar"},
                    b: {value: 5}
                  }
                }
              }
            }
          },
          action: 'invokeFunction',
          params: {
            FunctionName: { ref: 'stage.one' },
            Payload: { helper: 'one' ,
              params: {
                a: { ref: 'stage.one'},
                b: { value: 1},
              }
            }
          }
        },
        nextFunctionDisabled: {
          conditions: {
            doesNotMatch: {
              every: {
                doesMatch: {
                  helper: "matches",
                  params: {
                    a: {ref: "event.foo.bar"},
                    b: {value: 4}
                  }
                },
                doesNotMatch: {
                  helper: "matches",
                  params: {
                    a: {ref: "event.foo.bar"},
                    b: {value: 5}
                  }
                }
              }
            }
          },
          action: 'invokeFunction',
          params: {
            FunctionName: { ref: 'stage.one' },
            Payload: { helper: 'one' ,
              params: {
                a: { ref: 'stage.one'},
                b: { value: 1},
              }
            }
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
}

generateTests('Basic', [test1, test2, test3, test4, test5, test6, test7])
