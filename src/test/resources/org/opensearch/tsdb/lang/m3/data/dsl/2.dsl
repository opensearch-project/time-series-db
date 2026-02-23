{
  "size" : 0,
  "query" : {
    "bool" : {
      "should" : [
        {
          "time_range_pruner" : {
            "min_timestamp" : 999940000,
            "max_timestamp" : 1001000000,
            "query" : {
              "bool" : {
                "filter" : [
                  {
                    "range" : {
                      "timestamp_range" : {
                        "from" : 999940000,
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
                        "uuid:uuid-1",
                        "uuid:uuid-2",
                        "uuid:uuid-3"
                      ],
                      "boost" : 1.0
                    }
                  },
                  {
                    "cached_wildcard" : {
                      "wildcard" : {
                        "labels" : {
                          "wildcard" : "dc:sfo*",
                          "boost" : 1.0
                        }
                      }
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
            "min_timestamp" : 999940000,
            "max_timestamp" : 1001000000,
            "query" : {
              "bool" : {
                "filter" : [
                  {
                    "range" : {
                      "timestamp_range" : {
                        "from" : 999940000,
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
                        "uuid:uuid-1",
                        "uuid:uuid-2",
                        "uuid:uuid-3"
                      ],
                      "boost" : 1.0
                    }
                  },
                  {
                    "cached_wildcard" : {
                      "wildcard" : {
                        "labels" : {
                          "wildcard" : "dc:sjc*",
                          "boost" : 1.0
                        }
                      }
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
    "0" : {
      "filter" : {
        "time_range_pruner" : {
          "min_timestamp" : 999940000,
          "max_timestamp" : 1001000000,
          "query" : {
            "bool" : {
              "filter" : [
                {
                  "range" : {
                    "timestamp_range" : {
                      "from" : 999940000,
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
                      "uuid:uuid-1",
                      "uuid:uuid-2",
                      "uuid:uuid-3"
                    ],
                    "boost" : 1.0
                  }
                },
                {
                  "cached_wildcard" : {
                    "wildcard" : {
                      "labels" : {
                        "wildcard" : "dc:sfo*",
                        "boost" : 1.0
                      }
                    }
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
        "0_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 999940000,
            "max_timestamp" : 1001000000,
            "step" : 100000,
            "stages" : [
              {
                "type" : "sum",
                "group_by_labels" : [
                  "dc"
                ]
              }
            ]
          }
        }
      }
    },
    "4" : {
      "filter" : {
        "time_range_pruner" : {
          "min_timestamp" : 999940000,
          "max_timestamp" : 1001000000,
          "query" : {
            "bool" : {
              "filter" : [
                {
                  "range" : {
                    "timestamp_range" : {
                      "from" : 999940000,
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
                      "uuid:uuid-1",
                      "uuid:uuid-2",
                      "uuid:uuid-3"
                    ],
                    "boost" : 1.0
                  }
                },
                {
                  "cached_wildcard" : {
                    "wildcard" : {
                      "labels" : {
                        "wildcard" : "dc:sjc*",
                        "boost" : 1.0
                      }
                    }
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
            "min_timestamp" : 999940000,
            "max_timestamp" : 1001000000,
            "step" : 100000,
            "stages" : [
              {
                "type" : "sum",
                "group_by_labels" : [
                  "dc"
                ]
              }
            ]
          }
        }
      }
    },
    "0_coordinator" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "transform_null",
            "fill_value" : 0.0
          },
          {
            "type" : "moving",
            "interval" : 60000,
            "function" : "sum"
          },
          {
            "type" : "truncate",
            "truncate_start" : 1000000000,
            "truncate_end" : 1001000000
          }
        ],
        "references" : {
          "0_unfold" : "0>0_unfold"
        },
        "inputReference" : "0_unfold"
      }
    },
    "4_coordinator" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "transform_null",
            "fill_value" : 0.0
          },
          {
            "type" : "moving",
            "interval" : 60000,
            "function" : "sum"
          },
          {
            "type" : "truncate",
            "truncate_start" : 1000000000,
            "truncate_end" : 1001000000
          }
        ],
        "references" : {
          "4_unfold" : "4>4_unfold"
        },
        "inputReference" : "4_unfold"
      }
    },
    "8" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "as_percent",
            "right_op_reference" : "4"
          },
          {
            "type" : "scale",
            "factor" : 100.0
          },
          {
            "type" : "scale",
            "factor" : 0.01
          },
          {
            "type" : "alias",
            "pattern" : "exec"
          }
        ],
        "references" : {
          "0" : "0_coordinator",
          "4" : "4_coordinator"
        },
        "inputReference" : "0"
      }
    }
  }
}
