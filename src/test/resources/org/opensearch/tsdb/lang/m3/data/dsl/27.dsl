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
    "2" : {
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
        "2_unfold" : {
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
    "12" : {
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
        "12_unfold" : {
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
    "10" : {
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
        "10_unfold" : {
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
    "11_coordinator" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "_copy"
          }
        ],
        "references" : {
          "11_unfold" : "1>1_unfold"
        },
        "inputReference" : "11_unfold"
      }
    },
    "9_coordinator" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "_copy"
          }
        ],
        "references" : {
          "9_unfold" : "2>2_unfold"
        },
        "inputReference" : "9_unfold"
      }
    },
    "5" : {
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
          "1" : "1>1_unfold",
          "2" : "2>2_unfold"
        },
        "inputReference" : "1"
      }
    },
    "15" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "as_percent",
            "right_op_reference" : "9"
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
          "11" : "11_coordinator",
          "9" : "9_coordinator"
        },
        "inputReference" : "11"
      }
    },
    "20" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "as_percent",
            "right_op_reference" : "10"
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
          "12" : "12>12_unfold",
          "10" : "10>10_unfold"
        },
        "inputReference" : "12"
      }
    },
    "23" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "union",
            "right_op_reference" : "20"
          },
          {
            "type" : "min"
          },
          {
            "type" : "scale",
            "factor" : 1000.0000000000568
          }
        ],
        "references" : {
          "15" : "15",
          "20" : "20"
        },
        "inputReference" : "15"
      }
    },
    "26" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "subtract",
            "right_op_reference" : "23",
            "keep_nans" : false
          }
        ],
        "references" : {
          "23" : "23",
          "5" : "5"
        },
        "inputReference" : "5"
      }
    }
  }
}
