from schema import Schema, And, Optional

create_dataset_schema = Schema({
  "name": And(str),
  Optional("description"): str,
  "defaultProfileId": And(str),
  "schema": {
    Optional("tables"): [
      {
        "name": And(str),
        "columns": And([
          {
            "name": And(str),
            "datatype": And(str),
            Optional("array_of"): bool,
            Optional("required"): bool
          }
        ]),
        Optional("primaryKey"): [],
        Optional("partitionMode"): str,
        Optional("datePartitionOptions"): {
          "column": And(str)
        },
        Optional("intPartitionOptions"): {
          "column": And(str),
          "min": int,
          "max": int,
          "interval": int
        },
        "rowCount": int
      }
    ],
    Optional("relationships"): [
      {
        "name": And(str),
        "from": {
          "table": And(str),
          "column": And(str)
        },
        "to": {
          "table": And(str),
          "column": And(str)
        }
      }
    ],
    Optional("assets"): [
      {
        "name": And(str),
        "tables": [
          {
            "name": And(str),
            "columns": And([
              str
            ])
          }
        ],
        "rootTable": And(str),
        "rootColumn": And(str),
        Optional("follow"): [
          str
        ]
      }
    ]
  },
  Optional("region"): str,
  Optional("cloudPlatform"): str,
  Optional("enableSecureMonitoring"): bool,
  Optional("phsId"): str,
  Optional("experimentalSelfHosted"): bool,
  Optional("properties"): {},
  Optional("dedicatedIngestServiceAccount"): bool,
  Optional("experimentalPredictableFileIds"): bool,
  Optional("policies"): {
    Optional("stewards"): [
      str
    ],
    Optional("custodians"): [
      str
    ],
    Optional("snapshotCreators"): [
      str
    ]
  },
  Optional("tags"): [
    str
  ]
})
