import logging
import re
import time
import numpy as np
import pandas as pd
from datetime import date, datetime
from typing import Any, Optional


class InferTDRSchema:
    """
    A class to infer the schema for a table in TDR (Terra Data Repository) based on input metadata.
    """

    PYTHON_TDR_DATA_TYPE_MAPPING = {
        str: "string",
        "fileref": "fileref",
        bool: "boolean",
        bytes: "bytes",
        date: "date",
        datetime: "datetime",
        float: "float64",
        np.float64: "float64",
        int: "int64",
        np.int64: "int64",
        time: "time",
    }

    def __init__(
            self,
            input_metadata: list[dict],
            table_name: str,
            all_fields_non_required: bool = False,
            allow_disparate_data_types_in_column: bool = False,
            primary_key: Optional[str] = None
    ):
        """
        Initialize the InferTDRSchema class.

        Args:
            input_metadata (list[dict]): The input metadata to infer the schema from.
            table_name (str): The name of the table for which the schema is being inferred.
            all_fields_non_required (bool): A boolean indicating whether all columns should be set to non-required
                besides for primary key.
            primary_key (str): The name of the primary key column. Used to determine column should be required
            allow_disparate_data_types_in_column (bool): A boolean indicating whether force disparate data types in a
                column to be strings.
        """
        self.input_metadata = input_metadata
        self.table_name = table_name
        self.all_fields_non_required = all_fields_non_required
        self.primary_key = primary_key
        self.allow_disparate_data_types_in_column = allow_disparate_data_types_in_column

    def _check_type_consistency(self, key_value_type_mappings: dict) -> list[dict]:
        """
        Check if all values for each header are of the same type.

        Args:
            key_value_type_mappings (dict): A dictionary where the key is the header,
                and the value is a list of values for the header.

        Raises:
            Exception: If types do not match for any header.
        """
        matching = []

        disparate_header_info = []

        for header, values_for_header in key_value_type_mappings.items():
            # check if some values are lists while others are not (consider this a "mismatch" if so) while ignoring
            # "None" entries
            if (any(isinstance(item, list) for item in values_for_header if item is not None) and
                    not all(isinstance(item, list) for item in values_for_header if item is not None)):
                all_values_matching = False
            # if the row contains ONLY lists of items, check that all items in each list are of the same type (while
            # ignoring "None" entries)
            elif all(isinstance(item, list) for item in values_for_header if item is not None):
                # first get all substrings that have some values
                non_empty_substrings = [v for v in values_for_header if v]
                if non_empty_substrings:
                    # get one "type" from the list of values
                    first_match_type = type([v[0] for v in non_empty_substrings][0])
                    all_values_matching = all(
                        all(isinstance(item, first_match_type) for item in sublist) for sublist in non_empty_substrings
                    )
                else:
                    # if all "sub-lists" are empty, assume that all types are matching (all empty lists are handled
                    # below)
                    all_values_matching = True
            else:
                # find one value that's non-none to get the type to check against
                # specifically check if not "None" since we can have all zeroes, for example
                type_to_match_against = type([v for v in values_for_header if v is not None][0])
                # check if all the values in the list that are non-none match the type of the first entry
                all_values_matching = all(
                    isinstance(v, type_to_match_against) for v in values_for_header if v is not None
                )

            # If ALL rows for the header are none, force the type to be a string
            if all_values_matching and not any(values_for_header):
                matching.append({header: all_values_matching})
                disparate_header_info.append(
                    {
                        "header": header,
                        "force_to_string": True,
                    }
                )
            if not all_values_matching and self.allow_disparate_data_types_in_column:
                logging.info(
                    f"Not all data types matched for header '{header}' but forcing them to strings as "
                    f"'allow_disparate_data_types_in_column' setting is set to true"
                )
                matching.append({header: True})
                disparate_header_info.append(
                    {
                        "header": header,
                        "force_to_string": True,
                    }
                )
            else:
                matching.append({header: all_values_matching})  # type: ignore[dict-item]
                disparate_header_info.append(
                    {
                        "header": header,
                        "force_to_string": False,
                    }
                )

        # Returns true if all headers are determined to be "matching"
        problematic_headers = [
            d.keys()
            for d in matching
            if not list(d.values())[0]
        ]

        if problematic_headers:
            raise Exception(
                f"Not all values for the following headers are of the same type: {problematic_headers}. To force all"
                f" values in rows of a given column to be forced to the same type and bypass this error, re-run with "
                f"the 'force_disparate_rows_to_string' option set to true"
            )

        return disparate_header_info

    def _python_type_to_tdr_type_conversion(self, value_for_header: Any) -> str:
        """
        Convert Python data types to TDR data types.

        Args:
            value_for_header (Any): The value to determine the TDR type for.

        Returns:
            str: The TDR data type.
        """
        az_filref_regex = "^https.*sc-.*"
        gcp_fileref_regex = "^gs://.*"

        # Find potential file references
        if isinstance(value_for_header, str):
            az_match = re.search(pattern=az_filref_regex,
                                 string=value_for_header)
            gcp_match = re.search(
                pattern=gcp_fileref_regex, string=value_for_header)
            if az_match or gcp_match:
                return self.PYTHON_TDR_DATA_TYPE_MAPPING["fileref"]

        # Tried to use this to parse datetimes, but it was turning too many
        # regular ints into datetimes. Commenting out for now
        # try:
        #    date_or_time = parser.parse(value_for_header)
        #    return self.PYTHON_TDR_DATA_TYPE_MAPPING[type(date_or_time)]
        #    pass
        # except (TypeError, ParserError):
        #    pass

        if isinstance(value_for_header, list):
            # check for potential list of filerefs
            for v in value_for_header:
                if isinstance(v, str):
                    az_match = re.search(pattern=az_filref_regex, string=v)
                    gcp_match = re.search(pattern=gcp_fileref_regex, string=v)
                    if az_match or gcp_match:
                        return self.PYTHON_TDR_DATA_TYPE_MAPPING["fileref"]
            non_none_entry_in_list = [a for a in value_for_header if a is not None][0]
            return self.PYTHON_TDR_DATA_TYPE_MAPPING[type(non_none_entry_in_list)]

        # if none of the above special cases apply, just pass the type of the value to determine the TDR type
        return self.PYTHON_TDR_DATA_TYPE_MAPPING[type(value_for_header)]

    def _format_column_metadata(self, key_value_type_mappings: dict, disparate_header_info: list[dict]) -> list[dict]:
        """
        Generate the metadata for each column's header name, data type, and whether it's an array of values.

        Args:
            key_value_type_mappings (dict): A dictionary where the key is the header,
                and the value is a list of values for the header.

        Returns:
            list[dict]: A list of dictionaries containing column metadata.
        """
        columns = []

        for header, values_for_header in key_value_type_mappings.items():
            force_to_string = [h["force_to_string"] for h in disparate_header_info if h["header"] == header][0]

            # if the ANY of the values for a given header is a list, we assume that column contains arrays of values
            array_of = True if any(isinstance(v, list) for v in values_for_header) else False

            if force_to_string:
                logging.info(f"Header '{header}' was forced to string to to mismatched datatypes in column")
                data_type = self.PYTHON_TDR_DATA_TYPE_MAPPING[str]
            else:
                # find either the first item that's non-None, or the first non-empty list
                data_type = self._python_type_to_tdr_type_conversion([a for a in values_for_header if a is not None][0])

            column_metadata = {
                "name": header,
                "datatype": data_type,
                "array_of": array_of,
            }
            columns.append(column_metadata)

        return columns

    def _gather_required_and_non_required_headers(self, metadata_df: Any, dataframe_headers: list[str]) -> list[dict]:
        """
        Determine whether each header is required or not.

        Args:
            metadata_df (Any): The original dataframe.
            dataframe_headers (list[str]): A list of dataframe headers.

        Returns:
            list[dict]: A list of dictionaries containing header requirements.
        """
        header_requirements = []

        na_replaced = metadata_df.replace({None: np.nan})
        for header in dataframe_headers:
            all_none = na_replaced[header].isna().all()
            some_none = na_replaced[header].isna().any()
            contains_array = na_replaced[header].apply(lambda x: isinstance(x, (np.ndarray, list))).any()

            # if the column contains any arrays, set it as optional since arrays cannot be required in tdr
            if contains_array:
                header_requirements.append({"name": header, "required": False})
            # if all rows are none for a given column, we set the default type to "string" type in TDR
            elif all_none:
                header_requirements.append({"name": header, "required": False, "data_type": "string"})
            # if some rows are none or all non required is set to true AND header
            # is not primary key, we set the column to non-required
            elif some_none or (self.all_fields_non_required and header != self.primary_key):
                header_requirements.append({"name": header, "required": False})
            else:
                header_requirements.append({"name": header, "required": True})

        return header_requirements

    @staticmethod
    def _reformat_metadata(cleaned_metadata: list[dict]) -> dict:
        """
        Create a dictionary where the key is the header name, and the value is a list of all values for that header.

        Args:
            cleaned_metadata (list[dict]): The cleaned metadata.

        Returns:
            dict: A dictionary with header names as keys and lists of values as values.
        """
        key_value_type_mappings = {}
        unique_headers = {key for row in cleaned_metadata for key in row}

        for header in unique_headers:
            for row in cleaned_metadata:
                value = row[header]
                if header not in key_value_type_mappings:
                    key_value_type_mappings[header] = [value]
                else:
                    key_value_type_mappings[header].append(value)
        return key_value_type_mappings

    def infer_schema(self) -> dict:
        """
        Infer the schema for the table based on the input metadata.

        Returns:
            dict: The inferred schema in TDR format.
        """
        logging.info(f"Inferring schema for table {self.table_name}")
        # create the dataframe
        metadata_df = pd.DataFrame(self.input_metadata)
        # Replace all nan with None
        metadata_df = metadata_df.where(pd.notnull(metadata_df), None)

        # find all headers that need to be renamed if they have "entity" in them and rename the headers
        headers_to_be_renamed = [{h: h.split(":")[1] for h in list(metadata_df.columns) if h.startswith("entity")}][0]
        metadata_df = metadata_df.rename(columns=headers_to_be_renamed)

        # start by gathering the column metadata by determining which headers are required or not
        column_metadata = self._gather_required_and_non_required_headers(metadata_df, list(metadata_df.columns))

        # drop columns where ALL values are None, but keep rows where some values are None
        # we keep the rows where some values are none because if we happen to have a different column that's none in
        # every row, we could end up with no data at the end
        all_none_columns_dropped_df = metadata_df.dropna(axis=1, how="all")
        cleaned_metadata = all_none_columns_dropped_df.to_dict(orient="records")
        key_value_type_mappings = self._reformat_metadata(cleaned_metadata)

        # check to see if all values corresponding to a header are of the same type
        disparate_header_info = self._check_type_consistency(key_value_type_mappings)

        columns = self._format_column_metadata(
            key_value_type_mappings=key_value_type_mappings, disparate_header_info=disparate_header_info
        )

        # combine the information about required headers with the data types that were collected
        for header_metadata in column_metadata:
            matching_metadata = [d for d in columns if d["name"] == header_metadata["name"]]
            if matching_metadata:
                header_metadata.update(matching_metadata[0])

        tdr_tables_json = {
            "name": self.table_name,
            "columns": column_metadata,
        }

        return tdr_tables_json
