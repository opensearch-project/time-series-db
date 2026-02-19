{
  "size" : 0,
  "query" : {
    "bool" : {
      "should" : [
        {
          "time_range_pruner" : {
            "min_timestamp" : 999700000,
            "max_timestamp" : 1001000000,
            "query" : {
              "bool" : {
                "filter" : [
                  {
                    "range" : {
                      "timestamp_range" : {
                        "from" : 999700000,
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
                        "service:logger"
                      ],
                      "boost" : 1.0
                    }
                  },
                  {
                    "terms" : {
                      "labels" : [
                        "name:logs"
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
            "min_timestamp" : 999700000,
            "max_timestamp" : 1001000000,
            "query" : {
              "bool" : {
                "filter" : [
                  {
                    "range" : {
                      "timestamp_range" : {
                        "from" : 999700000,
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
                        "service:logger"
                      ],
                      "boost" : 1.0
                    }
                  },
                  {
                    "terms" : {
                      "labels" : [
                        "name:attempts"
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
            "min_timestamp" : 999700000,
            "max_timestamp" : 1001000000,
            "query" : {
              "bool" : {
                "filter" : [
                  {
                    "range" : {
                      "timestamp_range" : {
                        "from" : 999700000,
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
                        "service:logger"
                      ],
                      "boost" : 1.0
                    }
                  }
                ],
                "must_not" : [
                  {
                    "terms" : {
                      "labels" : [
                        "error_type:backoff"
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
            "min_timestamp" : 999700000,
            "max_timestamp" : 1001000000,
            "query" : {
              "bool" : {
                "filter" : [
                  {
                    "range" : {
                      "timestamp_range" : {
                        "from" : 999700000,
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
                        "service:logger"
                      ],
                      "boost" : 1.0
                    }
                  },
                  {
                    "cached_wildcard" : {
                      "wildcard" : {
                        "labels" : {
                          "wildcard" : "error_type:*",
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
          "min_timestamp" : 999700000,
          "max_timestamp" : 1001000000,
          "query" : {
            "bool" : {
              "filter" : [
                {
                  "range" : {
                    "timestamp_range" : {
                      "from" : 999700000,
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
                      "service:logger"
                    ],
                    "boost" : 1.0
                  }
                },
                {
                  "terms" : {
                    "labels" : [
                      "name:logs"
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
            "min_timestamp" : 999700000,
            "max_timestamp" : 1001000000,
            "step" : 100000,
            "stages" : [
              {
                "type" : "sum"
              }
            ]
          }
        }
      }
    },
    "2" : {
      "filter" : {
        "time_range_pruner" : {
          "min_timestamp" : 999700000,
          "max_timestamp" : 1001000000,
          "query" : {
            "bool" : {
              "filter" : [
                {
                  "range" : {
                    "timestamp_range" : {
                      "from" : 999700000,
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
                      "service:logger"
                    ],
                    "boost" : 1.0
                  }
                },
                {
                  "terms" : {
                    "labels" : [
                      "name:attempts"
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
            "min_timestamp" : 999700000,
            "max_timestamp" : 1001000000,
            "step" : 100000,
            "stages" : [
              {
                "type" : "sum"
              }
            ]
          }
        }
      }
    },
    "6" : {
      "filter" : {
        "time_range_pruner" : {
          "min_timestamp" : 999700000,
          "max_timestamp" : 1001000000,
          "query" : {
            "bool" : {
              "filter" : [
                {
                  "range" : {
                    "timestamp_range" : {
                      "from" : 999700000,
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
                      "service:logger"
                    ],
                    "boost" : 1.0
                  }
                }
              ],
              "must_not" : [
                {
                  "terms" : {
                    "labels" : [
                      "error_type:backoff"
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
        "6_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 999700000,
            "max_timestamp" : 1001000000,
            "step" : 100000,
            "stages" : [
              {
                "type" : "sum"
              }
            ]
          }
        }
      }
    },
    "8" : {
      "filter" : {
        "time_range_pruner" : {
          "min_timestamp" : 999700000,
          "max_timestamp" : 1001000000,
          "query" : {
            "bool" : {
              "filter" : [
                {
                  "range" : {
                    "timestamp_range" : {
                      "from" : 999700000,
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
                      "service:logger"
                    ],
                    "boost" : 1.0
                  }
                },
                {
                  "cached_wildcard" : {
                    "wildcard" : {
                      "labels" : {
                        "wildcard" : "error_type:*",
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
        "8_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 999700000,
            "max_timestamp" : 1001000000,
            "step" : 100000,
            "stages" : [
              {
                "type" : "sum"
              }
            ]
          }
        }
      }
    },
    "16_coordinator" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "_copy"
          }
        ],
        "references" : {
          "16_unfold" : "2>2_unfold"
        },
        "inputReference" : "16_unfold"
      }
    },
    "4" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "subtract",
            "right_op_reference" : "2",
            "keep_nans" : false
          },
          {
            "type" : "abs"
          }
        ],
        "references" : {
          "0" : "0>0_unfold",
          "2" : "2>2_unfold"
        },
        "inputReference" : "0"
      }
    },
    "10" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "divide",
            "right_op_reference" : "8",
            "labels" : [
              "error_type"
            ]
          },
          {
            "type" : "abs"
          },
          {
            "type" : "transform_null",
            "fill_value" : 0.0
          },
          {
            "type" : "moving",
            "interval" : 300000,
            "function" : "avg"
          },
          {
            "type" : "truncate",
            "truncate_start" : 1000000000,
            "truncate_end" : 1001000000
          }
        ],
        "references" : {
          "6" : "6>6_unfold",
          "8" : "8>8_unfold"
        },
        "inputReference" : "6"
      }
    },
    "14" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "union",
            "right_op_reference" : "10"
          },
          {
            "type" : "sum"
          }
        ],
        "references" : {
          "4" : "4",
          "10" : "10"
        },
        "inputReference" : "4"
      }
    },
    "18" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "as_percent",
            "right_op_reference" : "16"
          },
          {
            "type" : "moving",
            "interval" : 300000,
            "function" : "min"
          },
          {
            "type" : "alias",
            "pattern" : "percent"
          },
          {
            "type" : "truncate",
            "truncate_start" : 1000000000,
            "truncate_end" : 1001000000
          }
        ],
        "references" : {
          "14" : "14",
          "16" : "16_coordinator"
        },
        "inputReference" : "14"
      }
    }
  }
}
