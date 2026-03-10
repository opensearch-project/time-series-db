{
  "size" : 0,
  "query" : {
    "match_none" : {
      "boost" : 1.0
    }
  },
  "track_total_hits" : -1,
  "aggregations" : {
    "0" : {
      "filter" : {
        "match_none" : {
          "boost" : 1.0
        }
      },
      "aggregations" : {
        "0_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 1000000000,
            "max_timestamp" : 1001000000,
            "step" : 100000
          }
        }
      }
    },
    "1" : {
      "filter" : {
        "match_none" : {
          "boost" : 1.0
        }
      },
      "aggregations" : {
        "1_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 1000000000,
            "max_timestamp" : 1001000000,
            "step" : 100000
          }
        }
      }
    },
    "0_coordinator" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "mockFetch",
            "values" : [
              -10.0,
              20.0,
              30.0
            ],
            "tags" : {
              "name" : "a",
              "env" : "prod"
            },
            "startTime" : 1000000000,
            "endTime" : 1001000000,
            "step" : 100000
          }
        ],
        "references" : {
          "0_unfold" : "0>0_unfold"
        },
        "inputReference" : "0_unfold"
      }
    },
    "1_coordinator" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "mockFetchLine",
            "value" : -5.0,
            "tags" : {
              "name" : "b",
              "env" : "dev"
            },
            "startTime" : 1000000000,
            "endTime" : 1001000000,
            "step" : 100000
          }
        ],
        "references" : {
          "1_unfold" : "1>1_unfold"
        },
        "inputReference" : "1_unfold"
      }
    },
    "2" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "union",
            "right_op_reference" : "1"
          },
          {
            "type" : "scale",
            "factor" : 2.0
          },
          {
            "type" : "sum"
          }
        ],
        "references" : {
          "0" : "0_coordinator",
          "1" : "1_coordinator"
        },
        "inputReference" : "0"
      }
    }
  }
}
