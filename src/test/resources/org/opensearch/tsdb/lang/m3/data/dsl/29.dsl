{
  "size" : 0,
  "query" : {
    "bool" : {
      "should" : [
        {
          "time_range_pruner" : {
            "min_timestamp" : 913600000,
            "max_timestamp" : 1001000000,
            "query" : {
              "bool" : {
                "filter" : [
                  {
                    "range" : {
                      "timestamp_range" : {
                        "from" : 913600000,
                        "to" : 1001000000,
                        "include_lower" : true,
                        "include_upper" : false,
                        "boost" : 1.0
                      }
                    }
                  },
                  {
                    "terms" : {
                      "labels" : [
                        "name:errors"
                      ],
                      "boost" : 1.0
                    }
                  }
                ],
                "adjust_pure_negative" : true,
                "boost" : 1.0
              }
            },
            "boost" : 1.0
          }
        },
        {
          "time_range_pruner" : {
            "min_timestamp" : 913600000,
            "max_timestamp" : 1001000000,
            "query" : {
              "bool" : {
                "filter" : [
                  {
                    "range" : {
                      "timestamp_range" : {
                        "from" : 913600000,
                        "to" : 1001000000,
                        "include_lower" : true,
                        "include_upper" : false,
                        "boost" : 1.0
                      }
                    }
                  },
                  {
                    "terms" : {
                      "labels" : [
                        "name:total"
                      ],
                      "boost" : 1.0
                    }
                  }
                ],
                "adjust_pure_negative" : true,
                "boost" : 1.0
              }
            },
            "boost" : 1.0
          }
        },
        {
          "time_range_pruner" : {
            "min_timestamp" : 992800000,
            "max_timestamp" : 1001000000,
            "query" : {
              "bool" : {
                "filter" : [
                  {
                    "range" : {
                      "timestamp_range" : {
                        "from" : 992800000,
                        "to" : 1001000000,
                        "include_lower" : true,
                        "include_upper" : false,
                        "boost" : 1.0
                      }
                    }
                  },
                  {
                    "terms" : {
                      "labels" : [
                        "name:errors"
                      ],
                      "boost" : 1.0
                    }
                  }
                ],
                "adjust_pure_negative" : true,
                "boost" : 1.0
              }
            },
            "boost" : 1.0
          }
        },
        {
          "time_range_pruner" : {
            "min_timestamp" : 992800000,
            "max_timestamp" : 1001000000,
            "query" : {
              "bool" : {
                "filter" : [
                  {
                    "range" : {
                      "timestamp_range" : {
                        "from" : 992800000,
                        "to" : 1001000000,
                        "include_lower" : true,
                        "include_upper" : false,
                        "boost" : 1.0
                      }
                    }
                  },
                  {
                    "terms" : {
                      "labels" : [
                        "name:total"
                      ],
                      "boost" : 1.0
                    }
                  }
                ],
                "adjust_pure_negative" : true,
                "boost" : 1.0
              }
            },
            "boost" : 1.0
          }
        }
      ],
      "adjust_pure_negative" : true,
      "minimum_should_match" : "1",
      "boost" : 1.0
    }
  },
  "track_total_hits" : -1,
  "aggregations" : {
    "3" : {
      "filter" : {
        "time_range_pruner" : {
          "min_timestamp" : 913600000,
          "max_timestamp" : 1001000000,
          "query" : {
            "bool" : {
              "filter" : [
                {
                  "range" : {
                    "timestamp_range" : {
                      "from" : 913600000,
                      "to" : 1001000000,
                      "include_lower" : true,
                      "include_upper" : false,
                      "boost" : 1.0
                    }
                  }
                },
                {
                  "terms" : {
                    "labels" : [
                      "name:errors"
                    ],
                    "boost" : 1.0
                  }
                }
              ],
              "adjust_pure_negative" : true,
              "boost" : 1.0
            }
          },
          "boost" : 1.0
        }
      },
      "aggregations" : {
        "3_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 913600000,
            "max_timestamp" : 1001000000,
            "step" : 100000,
            "stages" : [
              {
                "type" : "moving",
                "interval" : 86400000,
                "function" : "sum"
              },
              {
                "type" : "truncate",
                "truncate_start" : 1000000000,
                "truncate_end" : 1001000000
              }
            ]
          }
        }
      }
    },
    "1" : {
      "filter" : {
        "time_range_pruner" : {
          "min_timestamp" : 913600000,
          "max_timestamp" : 1001000000,
          "query" : {
            "bool" : {
              "filter" : [
                {
                  "range" : {
                    "timestamp_range" : {
                      "from" : 913600000,
                      "to" : 1001000000,
                      "include_lower" : true,
                      "include_upper" : false,
                      "boost" : 1.0
                    }
                  }
                },
                {
                  "terms" : {
                    "labels" : [
                      "name:total"
                    ],
                    "boost" : 1.0
                  }
                }
              ],
              "adjust_pure_negative" : true,
              "boost" : 1.0
            }
          },
          "boost" : 1.0
        }
      },
      "aggregations" : {
        "1_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 913600000,
            "max_timestamp" : 1001000000,
            "step" : 100000,
            "stages" : [
              {
                "type" : "moving",
                "interval" : 86400000,
                "function" : "sum"
              },
              {
                "type" : "truncate",
                "truncate_start" : 1000000000,
                "truncate_end" : 1001000000
              }
            ]
          }
        }
      }
    },
    "4" : {
      "filter" : {
        "time_range_pruner" : {
          "min_timestamp" : 992800000,
          "max_timestamp" : 1001000000,
          "query" : {
            "bool" : {
              "filter" : [
                {
                  "range" : {
                    "timestamp_range" : {
                      "from" : 992800000,
                      "to" : 1001000000,
                      "include_lower" : true,
                      "include_upper" : false,
                      "boost" : 1.0
                    }
                  }
                },
                {
                  "terms" : {
                    "labels" : [
                      "name:errors"
                    ],
                    "boost" : 1.0
                  }
                }
              ],
              "adjust_pure_negative" : true,
              "boost" : 1.0
            }
          },
          "boost" : 1.0
        }
      },
      "aggregations" : {
        "4_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 992800000,
            "max_timestamp" : 1001000000,
            "step" : 100000,
            "stages" : [
              {
                "type" : "moving",
                "interval" : 7200000,
                "function" : "sum"
              },
              {
                "type" : "truncate",
                "truncate_start" : 1000000000,
                "truncate_end" : 1001000000
              }
            ]
          }
        }
      }
    },
    "2" : {
      "filter" : {
        "time_range_pruner" : {
          "min_timestamp" : 992800000,
          "max_timestamp" : 1001000000,
          "query" : {
            "bool" : {
              "filter" : [
                {
                  "range" : {
                    "timestamp_range" : {
                      "from" : 992800000,
                      "to" : 1001000000,
                      "include_lower" : true,
                      "include_upper" : false,
                      "boost" : 1.0
                    }
                  }
                },
                {
                  "terms" : {
                    "labels" : [
                      "name:total"
                    ],
                    "boost" : 1.0
                  }
                }
              ],
              "adjust_pure_negative" : true,
              "boost" : 1.0
            }
          },
          "boost" : 1.0
        }
      },
      "aggregations" : {
        "2_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 992800000,
            "max_timestamp" : 1001000000,
            "step" : 100000,
            "stages" : [
              {
                "type" : "moving",
                "interval" : 7200000,
                "function" : "sum"
              },
              {
                "type" : "truncate",
                "truncate_start" : 1000000000,
                "truncate_end" : 1001000000
              }
            ]
          }
        }
      }
    },
    "7" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "as_percent",
            "right_op_reference" : "1"
          },
          {
            "type" : "scale",
            "factor" : 10.000000000000568
          },
          {
            "type" : "transform_null",
            "fill_value" : 0.0
          }
        ],
        "references" : {
          "1" : "1>1_unfold",
          "3" : "3>3_unfold"
        },
        "inputReference" : "3"
      }
    },
    "12" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "as_percent",
            "right_op_reference" : "2"
          },
          {
            "type" : "scale",
            "factor" : 10.000000000000568
          },
          {
            "type" : "transform_null",
            "fill_value" : 0.0
          }
        ],
        "references" : {
          "2" : "2>2_unfold",
          "4" : "4>4_unfold"
        },
        "inputReference" : "4"
      }
    },
    "15" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "union",
            "right_op_reference" : "12"
          },
          {
            "type" : "min"
          }
        ],
        "references" : {
          "12" : "12",
          "7" : "7"
        },
        "inputReference" : "7"
      }
    }
  }
}
