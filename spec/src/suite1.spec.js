const _ = require('lodash')
const {generateTests} = require('./testHarness')
const uuid = require('uuid')

const test1 = {
  name: 'nothing',
  validators: {
    intro: {
      dependencies: {
      },
      dependencyInput: {}
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
      },
      dependencies: {
      }
    },
    main: {
      transformers: {
      },
      dependencies: {
      }
    },
    outro: {
      transformers: {
      },
      dependencies: {
      }
    },
  },
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
        not: (n) => n === true,
        toJson: (n) => _.isEqual(JSON.parse(n), {a: 6}),
        fromJson: (n) => _.isEqual(n, {a: 6}),
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
        toJson: {helper: "toJson",
          params: {
            a: {value: 6}
          },
        },
        fromJson: {helper: "fromJson",
          params: {
            string: {value: '{"a": 6}'}
          },
        },
        not: {not: {ref: 6} },
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
         conditions: {
        not: {not: {ref: 6} }
         },
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

const test8 = {
  name: 'test8',
  validators: {
    intro: {
      dependencies: {
      },
      dependencyInput: {}
    },
    main: {
      dependencies: {
      },
      dependencyInput: {}
    },
    outro: {
      dependencies: {
        recursion_invoke: (dep) => {
          console.log(JSON.stringify(dep))
          return (
            dep.accessSchema === true && dep.params.FunctionName.value === "self" &&
            dep.params.InvocationType.value === "Event" && _.isEqual(dep.params.Payload, {value:"{\"a\":4,\"b\":1,\"recursionDepth\":2}"}))

      },
    },
    dependencyInput: {}
    }
  },
  config: {
    intro: {
      transformers: {
      },
      dependencies: {
      }
    },
    main: {
      transformers: {
      },
      dependencies: {
      }
    },
    outro: {
      transformers: {
      },
      dependencies: {
        recursion: {
          action: 'recurse',
          params: {
            Payload: { all: {
                a: { ref: 'event.a'},
                b: { value: 1},
              }
            }
          }
        },
      },
    },
  },
  event: {
    a: 4
  },
  context: {
    invokedFunctionArn: "self"
  },
}

const test9 = {
  name: 'test9',
  validators: {
    intro: {
      dependencies: {
      },
      dependencyInput: {}
    },
    main: {
      dependencies: {
      },
      dependencyInput: {}
    },
    outro: {
      dependencies: {
      },
      dependencyInput: {}
    }
  },
  config: {
    intro: {
      transformers: {
      },
      dependencies: {
      }
    },
    main: {
      transformers: {
      },
      dependencies: {
      }
    },
    outro: {
      transformers: {
      },
      dependencies: {
        recursion: {
          action: 'recurse',
          params: {
            Payload: { all: {
                a: { ref: 'event.a'},
                b: { value: 1},
              }
            }
          }
        },
      },
    },
  },
  event: {
    a: 4,
    recursionDepth: 3,
  },
  context: {
    invokedFunctionArn: "self"
  },
}

const test10 = {
  name: 'test10',
  validators: {
    intro: {
      dependencies: {
        eventConfigured_invoke: (dep) => { 
          console.log(JSON.stringify(dep))
          return (
            dep.accessSchema === true && dep.params.FunctionName.value === "testEventConfigured" &&
            dep.params.InvocationType.value === "Event" && JSON.parse(dep.params.Payload.value).runId )

      },
      },
      dependencyInput: {
        uniqueId: _.identity
      }
    },
    main: {
      dependencies: {
      },
      dependencyInput: {
        s3Object: (m) => uuid.validate(m.fileName)
      }
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
        uniqueId: {helper: 'uuid'}
      },
      dependencies: {
        eventConfigured: {
          action: 'eventConfiguredInvocation',
          params: {
            FunctionName: {value: 'testEventConfigured'},
            config: {
              value: {
                intro: {},
                main: {},
                outro: {},
              }
            },
            resourceReferences: {
              value: {
                s3Object: {
                  all: {
                  fileName: {ref: 'prospectiveEvent.runId' }
                }
                }
              }
            },
            payloadValues: {
              all: {
                runId: { ref: 'stage.uniqueId' }
              }
            }
          }
        }
      }
    },
    main: {
      transformers: {
        s3Object: {ref: 'intro.resourceReferences.eventConfigured_resources.s3Object'},
      },
      dependencies: {
      }
    },
    outro: {
      transformers: {
      },
      dependencies: {
      }
    },
  },
  event: {},
}

generateTests('Basic', [test1, test2, test3, test4, test5, test6, test7, test8, test9, test10])
