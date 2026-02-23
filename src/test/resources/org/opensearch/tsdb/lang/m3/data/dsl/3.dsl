{
  "size" : 0,
  "query" : {
    "bool" : {
      "should" : [
        {
          "time_range_pruner" : {
            "min_timestamp" : 996400000,
            "max_timestamp" : 1001000000,
            "query" : {
              "bool" : {
                "filter" : [
                  {
                    "range" : {
                      "timestamp_range" : {
                        "from" : 996400000,
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
                        "region:west"
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
            "min_timestamp" : 910000000,
            "max_timestamp" : 914600000,
            "query" : {
              "bool" : {
                "filter" : [
                  {
                    "range" : {
                      "timestamp_range" : {
                        "from" : 910000000,
                        "to" : 914600000,
                        "include_lower" : true,
                        "include_upper" : false,
                        "boost" : 1.0
                      }
                    }
                  },
                  {
                    "terms" : {
                      "labels" : [
                        "region:west"
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
            "min_timestamp" : 823600000,
            "max_timestamp" : 828200000,
            "query" : {
              "bool" : {
                "filter" : [
                  {
                    "range" : {
                      "timestamp_range" : {
                        "from" : 823600000,
                        "to" : 828200000,
                        "include_lower" : true,
                        "include_upper" : false,
                        "boost" : 1.0
                      }
                    }
                  },
                  {
                    "terms" : {
                      "labels" : [
                        "region:west"
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
    "0" : {
      "filter" : {
        "time_range_pruner" : {
          "min_timestamp" : 996400000,
          "max_timestamp" : 1001000000,
          "query" : {
            "bool" : {
              "filter" : [
                {
                  "range" : {
                    "timestamp_range" : {
                      "from" : 996400000,
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
                      "region:west"
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
        "0_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 996400000,
            "max_timestamp" : 1001000000,
            "step" : 100000,
            "stages" : [
              {
                "type" : "sum",
                "group_by_labels" : [
                  "city_name"
                ]
              }
            ]
          }
        }
      }
    },
    "8" : {
      "filter" : {
        "time_range_pruner" : {
          "min_timestamp" : 910000000,
          "max_timestamp" : 914600000,
          "query" : {
            "bool" : {
              "filter" : [
                {
                  "range" : {
                    "timestamp_range" : {
                      "from" : 910000000,
                      "to" : 914600000,
                      "include_lower" : true,
                      "include_upper" : false,
                      "boost" : 1.0
                    }
                  }
                },
                {
                  "terms" : {
                    "labels" : [
                      "region:west"
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
        "8_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 910000000,
            "max_timestamp" : 914600000,
            "step" : 100000,
            "stages" : [
              {
                "type" : "sum",
                "group_by_labels" : [
                  "city_name"
                ]
              }
            ]
          }
        }
      }
    },
    "14" : {
      "filter" : {
        "time_range_pruner" : {
          "min_timestamp" : 823600000,
          "max_timestamp" : 828200000,
          "query" : {
            "bool" : {
              "filter" : [
                {
                  "range" : {
                    "timestamp_range" : {
                      "from" : 823600000,
                      "to" : 828200000,
                      "include_lower" : true,
                      "include_upper" : false,
                      "boost" : 1.0
                    }
                  }
                },
                {
                  "terms" : {
                    "labels" : [
                      "region:west"
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
        "14_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 823600000,
            "max_timestamp" : 828200000,
            "step" : 100000,
            "stages" : [
              {
                "type" : "sum",
                "group_by_labels" : [
                  "city_name"
                ]
              }
            ]
          }
        }
      }
    },
    "4_coordinator" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "_copy"
          },
          {
            "type" : "transform_null",
            "fill_value" : 0.0
          },
          {
            "type" : "moving",
            "interval" : 3600000,
            "function" : "sum"
          },
          {
            "type" : "truncate",
            "truncate_start" : 1000000000,
            "truncate_end" : 1001000000
          }
        ],
        "references" : {
          "4_unfold" : "0>0_unfold"
        },
        "inputReference" : "4_unfold"
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
            "interval" : 3600000,
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
    "8_coordinator" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "transform_null",
            "fill_value" : 0.0
          },
          {
            "type" : "moving",
            "interval" : 3600000,
            "function" : "sum"
          },
          {
            "type" : "timeshift",
            "shift_amount" : "86400000ms"
          },
          {
            "type" : "truncate",
            "truncate_start" : 1000000000,
            "truncate_end" : 1001000000
          }
        ],
        "references" : {
          "8_unfold" : "8>8_unfold"
        },
        "inputReference" : "8_unfold"
      }
    },
    "14_coordinator" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "transform_null",
            "fill_value" : 0.0
          },
          {
            "type" : "moving",
            "interval" : 3600000,
            "function" : "sum"
          },
          {
            "type" : "timeshift",
            "shift_amount" : "172800000ms"
          },
          {
            "type" : "truncate",
            "truncate_start" : 1000000000,
            "truncate_end" : 1001000000
          }
        ],
        "references" : {
          "14_unfold" : "14>14_unfold"
        },
        "inputReference" : "14_unfold"
      }
    },
    "13" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "union",
            "right_op_reference" : "8"
          },
          {
            "type" : "union",
            "right_op_reference" : "14"
          },
          {
            "type" : "avg"
          }
        ],
        "references" : {
          "14" : "14_coordinator",
          "4" : "4_coordinator",
          "8" : "8_coordinator"
        },
        "inputReference" : "4"
      }
    },
    "20" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "as_percent",
            "right_op_reference" : "13",
            "labels" : [
              "city_name",
              "region"
            ]
          },
          {
            "type" : "transform_null",
            "fill_value" : 0.0
          },
          {
            "type" : "moving",
            "interval" : 3600000,
            "function" : "avg"
          },
          {
            "type" : "truncate",
            "truncate_start" : 1000000000,
            "truncate_end" : 1001000000
          }
        ],
        "references" : {
          "0" : "0_coordinator",
          "13" : "13"
        },
        "inputReference" : "0"
      }
    }
  }
}
