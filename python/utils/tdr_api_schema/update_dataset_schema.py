from schema import Schema, And, Optional

update_schema = Schema({
  "description": And(str),
  "changes": And({
    "addTables": Optional([
      {
        "name": And(str),
        "columns": And([
          {
            "name": str,
            "datatype": str,
            "array_of": bool,
            "required": bool
          }
        ]),
        "primaryKey": Optional([
          str
        ]),
        "partitionMode": Optional(str),
        "datePartitionOptions": Optional({
          "column": str
        }),
        "intPartitionOptions": Optional({
          "column": str,
          "min": int,
          "max": int,
          "interval": int
        }),
        "rowCount": Optional(int)
      }
    ]),
    "addColumns": Optional([
      {
        "tableName": And(str),
        "columns": And([
          {
            "name": And(str),
            "datatype": And(str),
            "array_of": And(bool),
            "required": And(bool)
          }
        ])
      }
    ]),
    "addRelationships": Optional([
      {
        "name": And(str),
        "from": And({
          "table": And(str),
          "column": And(str)
        }),
        "to": And({
          "table": And(str),
          "column": And(str)
        })
      }
    ])
  })
})
