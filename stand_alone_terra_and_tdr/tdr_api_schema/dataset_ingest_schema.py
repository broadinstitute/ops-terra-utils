from schema import Schema, And, Use, Optional, SchemaError


update_schama = Schema({
  "table": And(str),
  Optional("path"): str,
  "records": And([]),
  "format": And(str),
  Optional("load_tag"): str,
  Optional("profile_id"): str,
  Optional("max_bad_records"): int,
  Optional("max_failed_file_loads"): int,
  Optional("ignore_unknown_values"): bool,
  Optional("csv_field_delimiter"): str,
  Optional("csv_quote"): str,
  Optional("csv_skip_leading_rows"): int,
  Optional("csv_allow_quoted_newlines"): bool,
  Optional("csv_null_marker"): str,
  Optional("csv_generate_row_ids"): bool,
  Optional("resolve_existing_files"): bool,
  Optional("transactionId"): str,
  Optional("updateStrategy"): str,
  Optional("bulkMode"): bool
})