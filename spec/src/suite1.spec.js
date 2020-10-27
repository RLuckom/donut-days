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
        not: (n) => n === true,
        toJson: (n) => _.isEqual(JSON.parse(n), {a: 6}),
        fromJson: (n) => _.isEqual(n, {a: 6}),
        qualifiedDependencyName: (n) => _.isEqual(n, 'a_b'),
        template: (n) => _.isEqual(n, 'a'),
        mapTemplate: (n) => _.isEqual(n, ['a', 'b']),
        isInList: (n) => _.isEqual(n, true),
        isNotInList: (n) => _.isEqual(n, false),
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
    stages: {
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
          isNotInList: {
            helper: 'isInList',
            params: {
              list: { value: ['a'] },
              item: { value: 'b'}
            }
          },
          template: {
            helper: 'template',
            params: {
              templateString: {value: '<%= a %>'},
              templateArguments: {value: {a: 'a'}}
            }
          },
          mapTemplate: {
            helper: 'mapTemplate',
            params: {
              templateString: {value: '<%= a %>'},
              templateArgumentsArray: {value: [{value: {a: 'a'}}, {value: {a: 'b'}}]}
            }
          },
          isInList: {
            helper: 'isInList',
            params: {
              list: {value: ['a']},
              item: {value: 'a'}
            }
          },
          qualifiedDependencyName: {
            helper: 'qualifiedDependencyName',
            params: {
              configStepName: {value: 'a'},
              dependencyName: {value: 'b'}
            }
          },
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
            condition: { not: {ref: 6} },
            action: 'one'
          }
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

const test31 = {
  name: 'test31',
  validators: {
    intro: {
      dependencyInput: {
        one: (n) => n === 4,
      },
      dependencies: {
      }
    },
    main: {
    },
    outro: {
      dependencies: {
      }
    },
    cleanup: {
      dependencyInput: {
        one: (n) => n === 4,
      },
    },
  },
  config: {
    condition: {
      helper : 'matches',
      params:  {
        a: {ref: 'event.foo.bar'},
        b: {value: 4}
      }
    },
    stages: {
      intro: {
        transformers: {
          one: {ref: 'event.foo.bar'},
        }
      }
    },
    cleanup: {
      transformers: {
        one: {ref: 'event.foo.bar'},
      }
    }
  },
  event: {
    foo: {
      bar: 4
    }
  },
  output: {one: 4},
  onComplete: (finishedSteps) => expect(finishedSteps.length).toEqual(1),
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
    condition: {
      helper : 'matches',
      params:  {
        a: {ref: 'event.foo.bar'},
        b: {value: 4}
      }
    },
  },
  event: {
    foo: {
      bar: 4
    }
  },
  onComplete: (finishedSteps) => expect(finishedSteps.length).toEqual(0),
}

const test4 = {
  name: 'test40',
  dependencyHelpers: {
    one: (params, addDependency) => addDependency('two', 'three') 
  },
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
    condition: {
      helper : 'matches',
      params:  {
        a: {ref: 'event.foo.bar'},
        b: {value: 4}
      }
    },
    stages: {
      intro: {
        transformers: {
          one: { value: 1}
        },
        dependencies: {
          foo: { action: 'one'}
        }
      }
    },
  },
  event: {
    foo: {
      bar: 7
    }
  },
  onComplete: (finishedSteps) => expect(finishedSteps.length).toEqual(0),
}

const test41 = {
  name: 'test41',
  helperFunctions: {
    one: ({a, b}) => a + b
  },
  validators: {
    intro: {
      dependencies: {
        nextFunction: (dep) => {
          return (dep.accessSchema === true && dep.params.FunctionName.value === 4
                  && dep.params.Payload.value === 5)
        },
      },
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
    stages: {
      intro: {
        condition: {
          helper : 'matches',
          params:  {
            a: {ref: 'event.foo.bar'},
            b: {value: 4}
          }
        },
        transformers: {
          one: {ref: 'event.foo.bar'},
          mediaId: {helper: 'uuid'},
        },
        dependencies: {
          nextFunction: {
            action: 'exploranda',
            params: {
              accessSchema: { value: 'dataSources.AWS.lambda.invoke'},
              params: {
                explorandaParams: {
                  FunctionName: { ref: 'stage.one'},
                  Payload: { helper: 'one' ,
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
      }
    }
  },
  event: {
    foo: {
      bar: 4
    }
  },
  onComplete: (finishedSteps) => expect(finishedSteps.length).toEqual(1),
}

const test42 = {
  name: 'test42',
  helperFunctions: {
    one: ({a, b}) => a + b
  },
  validators: {
    intro: {
      dependencyInput: {
        one: (n) => n === 7,
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
    stages: {
      intro: {
        condition: {
          helper : 'matches',
          params:  {
            a: {ref: 'event.foo.bar'},
            b: {value: 4}
          }
        },
        transformers: {
          one: {ref: 'event.foo.bar'},
          mediaId: {helper: 'uuid'},
        },
        dependencies: {
          nextFunction: {
            action: 'exploranda',
            params: {
              accessSchema: { value: 'dataSources.AWS.lambda.invoke'},
              params: {
                explorandaParams: {
                  FunctionName: { ref: 'stage.one'},
                  Payload: { helper: 'one' ,
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
      }
    }
  },
  event: {
    foo: {
      bar: 7
    }
  },
  onComplete: (finishedSteps) => expect(finishedSteps.length).toEqual(1),
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
    condition: {
      helper : 'matches',
      params:  {
        a: {ref: 'event.foo.bar'},
        b: {value: 4}
      }
    },
    stages: {
    intro: {
      transformers: {
        one: {ref: 'event.foo.bar'},
        mediaId: {helper: 'uuid'},
      },
      dependencies: {
      }
    }
    }
  },
  event: {
    foo: {
      bar: 4
    }
  },
  onComplete: (finishedSteps) => expect(finishedSteps.length).toEqual(1),
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
          return (dep.accessSchema === true && dep.params.FunctionName.value === 4
                  && dep.params.Payload.value === 5)
        },
        accessSchemaFunction_nextFunction: (dep) => {
          return (dep.accessSchema.foo === 'bar' && dep.accessSchema.baz() === 'qux' && dep.params.FunctionName.value === 4
                  && dep.params.Payload.value === 5)
        }
      }
    }
  },
  config: {
    condition: {
      helper : 'matches',
      params:  {
        a: {ref: 'event.foo.bar'},
        b: {value: 4}
      }
    },
    stages: {
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
          accessSchemaFunction: {
            action: 'explorandaDeprecated',
            params: {
              dependencyName: { value: 'nextFunction' },
              accessSchema: { value: {foo: 'bar', baz: () => 'qux'}},
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
          nextFunction: {
            condition: {
              helper: 'matches',
              params: {
                a: {ref: 'intro.vars.one'},
                b: {value: 4}
              }
            },
            action: 'explorandaDeprecated',
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
            condition: {
              helper: 'matches',
              params: {
                a: {ref: 'intro.vars.one'},
                b: {value: 4}
              }
            },
            action: 'explorandaDeprecated',
            dryRun: { value: true },
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
        },
      },
    },
  },
  event: {
    foo: {
      bar: 4
    }
  },
  onComplete: (finishedSteps) => expect(finishedSteps.length).toEqual(2),
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
    condition: {
      helper : 'matches',
      params:  {
        a: {ref: 'event.foo.bar'},
        b: {value: 4}
      }
    },
    stages: {
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
          condition: {
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
          condition: {
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
  },
  event: {
    foo: {
      bar: 4
    }
  },
  onComplete: (finishedSteps) => expect(finishedSteps.length).toEqual(2),
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
        recursion: (dep) => {
          return (
            dep.accessSchema === true && dep.params.FunctionName.value === "self" &&
            dep.params.InvocationType.value === "Event" && _.isEqual(dep.params.Payload, {value:"{\"a\":4,\"b\":1,\"recursionDepth\":2}"}))

      },
    },
    dependencyInput: {}
    }
  },
  config: {
    stages: {
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
    stages: {
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
  },
  event: {
    a: 4,
    recursionDepth: 3,
  },
  context: {
    invokedFunctionArn: "self"
  },
}

const test91 = {
  name: 'test91',
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
    overrides: {
      MAX_RECURSION_DEPTH: 2
    },
    stages: {
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
  },
  event: {
    a: 4,
    recursionDepth: 2,
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
        eventConfigured: (dep) => { 
          const payload = JSON.parse(dep.params.Payload.value)

          return (
            dep.accessSchema === true && dep.params.FunctionName.value === "testEventConfigured" &&
              dep.params.InvocationType.value === "Event" && uuid.validate(payload.event.runId) && uuid.validate(payload.config.expectations.s3Object.expectedResource.fileName)
          )
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
    stages: {
    intro: {
      transformers: {
        uniqueId: {helper: 'uuid'}
      },
      dependencies: {
        eventConfigured: {
          action: 'eventConfiguredDD',
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
                  fileName: {ref: 'stage.uniqueId' }
                }
                }
              }
            },
            event: {
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
  },
  event: {},
}

const test11 = {
  name: 'test11',
  expectError: true,
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
        fulfill_fulfillObject: (dep) => _.isEqual(dep, {})
      },
      dependencyInput: {}
    }
  },
  config: {
    expectations: {
      fulfilled: {
        expectedResource: {
          bucket: 'foo',
          key: 'bar'
        }
      }
    },
    stages: {
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
        fulfill: {
          action: 'fulfillObject',
          params: {
          }
        },
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
  dependencyHelpers: {
    fulfillObject: (params, addDependency, addResourceReference, getDependencyName, processParamsPreset, processParamValue, addFullfilledResource, transformers) => {
      addFullfilledResource({bucket: 'fro', key: 'bar'})
      addDependency('fulfillObject', params)
    }
  }
}

const test12 = {
  name: 'test12',
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
        fulfill_fulfillObject: (dep) => _.isEqual(dep, {})
      },
      dependencyInput: {}
    }
  },
  config: {
    expectations: {
      fulfilled: {
        expectedResource: {
          bucket: 'foo',
          key: 'bar'
        }
      }
    },
    stages: {
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
        fulfill: {
          action: 'fulfillObject',
          params: {
          }
        },
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
  dependencyHelpers: {
    fulfillObject: (params, addDependency, addResourceReference, getDependencyName, processParamsPreset, processParamValue, addFullfilledResource, transformers) => {
      addFullfilledResource({bucket: 'foo', key: 'bar'})
      addDependency('fulfillObject', params)
    }
  }
}

const test13 = {
  name: 'test13',
  validators: {
    intro: {
      dependencies: {
        dd: (dep) => { 
          const payload = JSON.parse(dep.params.Payload.value)

          return (
            dep.accessSchema === true && dep.params.FunctionName.value === "testdd" &&
              dep.params.InvocationType.value === "Event" && uuid.validate(payload.event.runId) && uuid.validate(payload.expectations.s3Object.expectedResource.fileName)
          )
        },
        dd2: (dep) => { 
          const payload = JSON.parse(dep.params.Payload.value)

          return (
            dep.accessSchema === true && dep.params.FunctionName.value === "testdd" &&
              dep.params.InvocationType.value === "RequestResponse" && uuid.validate(payload.event.runId)
          )
        },
      },
      dependencyInput: {
        uniqueId: _.identity
      }
    },
    main: {
      dependencies: {
        dd: (dep) => { 
          const payload = JSON.parse(dep.params.Payload.value)

          return (
            dep.accessSchema === true && dep.params.FunctionName.value === "testdd" &&
              dep.params.InvocationType.value === "Event" && uuid.validate(payload.event.runId) && uuid.validate(payload.expectations.s3Object.expectedResource.fileName)
          )
        },
        dd2: (dep) => { 
          const payload = JSON.parse(dep.params.Payload.value)

          return (
            dep.accessSchema === true && dep.params.FunctionName.value === "testdd" &&
              dep.params.InvocationType.value === "RequestResponse" && uuid.validate(payload.event.runId)
          )
        },
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
    stages: {
    main: {
      index: 1,
      transformers: {
        s3Object: {ref: 'intro.resourceReferences.dd_resources.s3Object'},
      },
      dependencies: {
        dd: {
          action: 'DD',
          params: {
            FunctionName: {value: 'testdd'},
            resourceReferences: {
              value: {
                s3Object: {
                  all: {
                  fileName: {ref: 'intro.vars.uniqueId' }
                }
                }
              }
            },
            event: {
              all: {
                runId: { ref: 'intro.vars.uniqueId' }
              }
            }
          }
        },
        dd2: {
          action: 'DD',
          params: {
            FunctionName: {value: 'testdd'},
            InvocationType: {value: 'RequestResponse'},
            event: {
              all: {
                runId: { ref: 'intro.vars.uniqueId' }
              }
            }
          }
        },
      }
    },
    intro: {
      index: 0,
      transformers: {
        uniqueId: {helper: 'uuid'}
      },
      dependencies: {
        dd: {
          action: 'DD',
          params: {
            FunctionName: {value: 'testdd'},
            resourceReferences: {
              value: {
                s3Object: {
                  all: {
                  fileName: {ref: 'stage.uniqueId' }
                }
                }
              }
            },
            event: {
              all: {
                runId: { ref: 'stage.uniqueId' }
              }
            }
          }
        },
        dd2: {
          action: 'DD',
          params: {
            FunctionName: {value: 'testdd'},
            InvocationType: {value: 'RequestResponse'},
            event: {
              all: {
                runId: { ref: 'stage.uniqueId' }
              }
            }
          }
        },
      }
    },
    outro: {
      index: 2,
      transformers: {
      },
      dependencies: {
      }
    },
  },
  },
  event: {},
}

generateTests('Basic', [test1, test2, test3, test31, test4, test41, test42, test5, test6, test7, test8, test9, test91, test10, test11, test12, test13])
