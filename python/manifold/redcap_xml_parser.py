#!/usr/bin/env python3
"""
REDCap XML Parser

This script processes REDCap XML data files and performs several key operations:
1. Parses XML data using xmltodict and ElementTree
2. Extracts schema information from the XML
3. Builds a schema dictionary with table and column details
4. Creates a decoding dictionary for coded values
5. Processes the actual clinical data from the XML
6. Generates table records with decoded values where applicable

The script is designed to be modular and easily broken into separate utility files if needed.
"""

import os
import re
import pandas as pd
import xmltodict
from typing import Dict, List, Any, Optional, Tuple
import logging
import argparse
import urllib.parse
import boto3

# Configure logging
logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class SchemaProcessor:
    """
    Processes the schema information from REDCap XML data.

    This class is responsible for extracting and processing schema information
    from REDCap XML data, including tables, columns, and their properties.
    """

    def __init__(self, parsed_data: Dict[str, Any]):
        """
        Initialize the SchemaProcessor with parsed XML data.

        Args:
            parsed_data: Dictionary containing parsed XML data
        """
        self.parsed_data = parsed_data
        self.forms = self._ensure_list(parsed_data["ODM"]["Study"]["MetaDataVersion"]["FormDef"])
        self.item_groups = self._ensure_list(parsed_data["ODM"]["Study"]["MetaDataVersion"]["ItemGroupDef"])
        self.items = self._ensure_list(parsed_data["ODM"]["Study"]["MetaDataVersion"]["ItemDef"])
        self.common_fields = self._initialize_common_fields()
        self.schema_dict: Dict[str, Dict[str, Any]] = {}

    def _ensure_list(self, data: Any) -> List[Any]:
        """
        Ensure that data is a list, even if it's a single item.

        Args:
            data: Data that should be a list

        Returns:
            List containing the data
        """
        return data if isinstance(data, list) else [data]

    def _initialize_common_fields(self) -> Dict[str, Dict[str, Any]]:
        """
        Initialize common fields that will be added to all tables.

        Returns:
            Dictionary of common fields with their properties
        """
        return {
            "mf_com_study_oid": {
                "column_name": "study_oid",
                "column_type": "text",
                "identifier": None,
                "required": "Yes",
                "data_type": "text",
                "data_length": "999",
                "code_list_oid": None
            },
            "mf_com_subject_key": {
                "column_name": "subject_key",
                "column_type": "text",
                "identifier": None,
                "required": "Yes",
                "data_type": "text",
                "data_length": "999",
                "code_list_oid": None
            },
            "mf_com_study_event": {
                "column_name": "study_event",
                "column_type": "text",
                "identifier": None,
                "required": "Yes",
                "data_type": "text",
                "data_length": "999",
                "code_list_oid": None
            }
        }

    def get_item_group(self, item_group_oid: str) -> Dict[str, Any]:
        """
        Look up an ItemGroupDef by its OID.

        Args:
            item_group_oid: OID of the item group to find

        Returns:
            Dictionary containing the item group data, or empty dict if not found
        """
        for item_group in self.item_groups:
            if item_group["@OID"] == item_group_oid:
                return item_group
        return {}

    def get_item(self, item_oid: str) -> Dict[str, Any]:
        """
        Look up an ItemDef by its OID.

        Args:
            item_oid: OID of the item to find

        Returns:
            Dictionary containing the item data, or empty dict if not found
        """
        for item in self.items:
            if item["@OID"] == item_oid:
                return item
        return {}

    def build_schema(self) -> Dict[str, Dict[str, Any]]:
        """
        Build a schema dictionary from the parsed XML data.

        Returns:
            Dictionary containing schema information for all forms
        """
        # Iterate through each form in the REDCap XML data
        # Each form will become a table in our schema
        for form in self.forms:
            # Extract the form's OID (unique identifier) and name
            # The OID will be used as a key in our schema dictionary
            form_oid = form["@OID"]
            # The form name will be used as the table name in the database
            table_name = form["@redcap:FormName"]
            # Start with common fields that all tables should have (study_oid, subject_key, etc.)
            # Make a copy to avoid modifying the original common_fields dictionary
            table_columns = self.common_fields.copy()

            # Get all item group references for this form
            # Item groups are collections of related items (fields) within a form
            # Ensure we have a list even if there's only one item group
            item_group_refs = self._ensure_list(form.get("ItemGroupRef", []))

            # Process each item group reference
            for item_group_ref in item_group_refs:
                # Get the actual item group object using its OID
                # This contains the details of the item group
                item_group_ref_obj = self.get_item_group(item_group_ref.get("@ItemGroupOID", ""))

                # Only process if we found a valid item group
                if item_group_ref_obj:
                    # Get all item references within this item group
                    # Items are the actual fields/questions in the form
                    # Ensure we have a list even if there's only one item
                    item_refs = self._ensure_list(item_group_ref_obj.get("ItemRef", []))

                    # Process each item reference to extract field details
                    for item_ref in item_refs:
                        # Get the item's OID and whether it's required
                        item_oid = item_ref.get("@ItemOID", "")
                        required = item_ref.get("@Mandatory", "")
                        # Get the actual item object using its OID
                        # This contains the detailed metadata about the field
                        item_ref_obj = self.get_item(item_oid)

                        # Only process if we found a valid item
                        if item_ref_obj:
                            # Extract field metadata from the item object
                            # These properties define the column in our schema
                            column_name = item_ref_obj.get("@Name", "")  # Field name
                            column_type = item_ref_obj.get("@redcap:FieldType", "")  # Field type (text, radio, etc.)
                            # Whether this is an identifier field
                            identifier = item_ref_obj.get("@redcap:Identifier", None)
                            data_type = item_ref_obj.get("@DataType", "")  # Data type (string, integer, etc.)
                            data_length = item_ref_obj.get("@Length", "")  # Maximum length of the field

                            # Check if this item has a code list reference
                            # Code lists are used for fields with predefined options (like dropdowns)
                            code_ref = item_ref_obj.get("CodeListRef", None)
                            # Get the code list OID if it exists, otherwise None
                            code_ref_oid = code_ref.get('@CodeListOID') if code_ref else None

                            # Add this field to our table columns dictionary
                            # Using the item OID as the key and all extracted metadata as values
                            table_columns.update({
                                item_oid: {
                                    "column_name": column_name,
                                    "column_type": column_type,
                                    "identifier": identifier,
                                    "required": required,
                                    "data_type": data_type,
                                    "data_length": data_length,
                                    "code_list_oid": code_ref_oid
                                }
                            })

            # After processing all items for this form, add the complete table definition
            # to our schema dictionary using the form OID as the key
            self.schema_dict[form_oid] = {"table_name": table_name, "columns": table_columns}

        # Return the complete schema dictionary containing all forms and their fields
        return self.schema_dict

    def get_schema_dataframe(self) -> pd.DataFrame:
        """
        Convert the schema dictionary to a pandas DataFrame.

        The DataFrame includes only the necessary columns for Snowflake table creation:
        - table_name: The name of the table
        - column_name: The name of the column
        - required: Whether the column is required (not nullable)
        - snowflake_data_type: The Snowflake data type to use for the column
        - primary_key: Whether the column is a primary key

        Returns:
            DataFrame containing schema information
        """
        schema_results = []

        for form_oid, table_details in self.schema_dict.items():
            for item_oid, column_details in table_details["columns"].items():
                # Map REDCap data types to Snowflake data types
                data_type = column_details["data_type"]
                data_length = column_details["data_length"]
                column_type = column_details["column_type"]

                # Default to VARCHAR
                snowflake_data_type = "VARCHAR"

                # Map data types to Snowflake data types
                if data_type == "integer":
                    snowflake_data_type = "INTEGER"
                elif data_type == "float":
                    snowflake_data_type = "FLOAT"
                elif data_type == "date":
                    snowflake_data_type = "DATE"
                elif data_type == "datetime":
                    snowflake_data_type = "TIMESTAMP_NTZ"
                elif data_type == "boolean" or column_type == "checkbox":
                    snowflake_data_type = "BOOLEAN"
                elif data_type == "text":
                    # Use data_length if available, otherwise default to VARCHAR(16777216)
                    if data_length and data_length.isdigit():
                        snowflake_data_type = f"VARCHAR({data_length})"
                    else:
                        snowflake_data_type = "VARCHAR(16777216)"  # Snowflake's maximum VARCHAR size

                # Determine if this is a primary key
                # For simplicity, we'll consider the study_id as the primary key
                primary_key = column_details["column_name"] == "study_id"

                # For now, we're not setting up foreign keys
                foreign_key = False
                foreign_key_table = None
                foreign_key_column = None

                schema_results.append({
                    "table_name": table_details["table_name"],
                    "column_name": column_details["column_name"],
                    "required": column_details["required"],
                    "snowflake_data_type": snowflake_data_type,
                    "primary_key": primary_key
                })

        return pd.DataFrame.from_dict(schema_results, orient='columns')


class DecodingProcessor:
    """
    Processes the decoding information from REDCap XML data.

    This class is responsible for extracting and processing decoding information
    from REDCap XML data, including code lists and their values.
    """

    def __init__(self, parsed_data: Dict[str, Any]):
        """
        Initialize the DecodingProcessor with parsed XML data.

        Args:
            parsed_data: Dictionary containing parsed XML data
        """
        self.parsed_data = parsed_data
        self.code_lists = self._ensure_list(parsed_data["ODM"]["Study"]["MetaDataVersion"].get("CodeList", []))
        self.decoding_dict: Dict[str, Dict[str, str]] = {}

    def _ensure_list(self, data: Any) -> List[Any]:
        """
        Ensure that data is a list, even if it's a single item.

        Args:
            data: Data that should be a list

        Returns:
            List containing the data
        """
        return data if isinstance(data, list) else [data]

    def parse_checkbox_choices(self, input_str: str) -> Dict[str, str]:
        """
        Parse checkbox choices from a string.

        Args:
            input_str: String containing checkbox choices

        Returns:
            Dictionary mapping choice codes to descriptions
        """
        parsed_input = {}
        items = input_str.split("|")

        for item in items:
            parts = item.strip().split(",", 1)
            if len(parts) == 2:
                key = parts[0].strip()
                val = parts[1].strip()
                parsed_input[key] = val

        return parsed_input

    def build_decoding_dict(self) -> Dict[str, Dict[str, str]]:
        """
        Build a decoding dictionary from the parsed XML data.

        Returns:
            Dictionary mapping code list OIDs to dictionaries of codes and descriptions
        """
        for code_list in self.code_lists:
            code_list_oid = code_list.get("@OID", "")
            code_list_items = self._ensure_list(code_list.get("CodeListItem", []))
            checkbox_field = True if code_list.get("@redcap:CheckboxChoices") else False
            code_list_dict = {}

            if checkbox_field:
                choices_str = code_list.get("@redcap:CheckboxChoices", "")
                parsed_choices = self.parse_checkbox_choices(choices_str)
                match = re.search(r'(\d+).choices$', code_list_oid)
                if match:
                    code_list_choice = match.group(1)
                    for key, val in parsed_choices.items():
                        if key == code_list_choice:
                            code_list_dict[key] = val
            else:
                for code_list_item in code_list_items:
                    code_list_dict[code_list_item["@CodedValue"]] = code_list_item["Decode"]["TranslatedText"]

            self.decoding_dict[code_list_oid] = code_list_dict

        return self.decoding_dict

    def get_decoding_dataframe(self) -> pd.DataFrame:
        """
        Convert the decoding dictionary to a pandas DataFrame.

        Returns:
            DataFrame containing decoding information
        """
        decoding_results = []

        for code_list_oid, code_list in self.decoding_dict.items():
            for code, desc in code_list.items():
                decoding_results.append({
                    "code_list_oid": code_list_oid,
                    "code": code,
                    "description": desc
                })

        return pd.DataFrame.from_dict(decoding_results, orient='columns')


class DataProcessor:
    """
    Processes the clinical data from REDCap XML data.

    This class is responsible for extracting and processing clinical data
    from REDCap XML data, including subject data, study events, and form data.
    """

    def __init__(
        self,
        parsed_data: Dict[str, Any],
        schema_dict: Dict[str, Dict[str, Any]],
        decoding_dict: Dict[str, Dict[str, str]]
    ):
        """
        Initialize the DataProcessor with parsed XML data, schema, and decoding information.

        Args:
            parsed_data: Dictionary containing parsed XML data
            schema_dict: Dictionary containing schema information
            decoding_dict: Dictionary containing decoding information
        """
        self.parsed_data = parsed_data
        self.schema_dict = schema_dict
        self.decoding_dict = decoding_dict
        self.table_records: Dict[str, List[Dict[str, Any]]] = {}

    def _ensure_list(self, data: Any) -> List[Any]:
        """
        Ensure that data is a list, even if it's a single item.

        Args:
            data: Data that should be a list

        Returns:
            List containing the data
        """
        return data if isinstance(data, list) else [data]

    def _create_base_record(self, study_oid: str, subject_key: str, study_event: str) -> Dict[str, Any]:
        """
        Create a new record with common fields.

        Args:
            study_oid: Study OID value
            subject_key: Subject key value
            study_event: Study event value

        Returns:
            Dictionary containing the base record with common fields
        """
        return {
            "study_oid": study_oid,
            "subject_key": subject_key,
            "study_event": study_event
        }

    def _decode_checkbox_field(self, column_name: str, item_raw_value: str,
                               code_list_oid: str) -> Any:
        """
        Decode a checkbox field value.

        Args:
            column_name: Name of the column
            item_raw_value: Raw value from the XML
            code_list_oid: OID of the code list for decoding

        Returns:
            Decoded value or None if checkbox is not checked
        """
        # For checkboxes, a value of "1" means the box is checked
        if item_raw_value == "1":
            # Extract the checkbox number from the column name (e.g., "checkbox_3" -> "3")
            match = re.search(r'(\d+)$', column_name)
            if match:
                # Get the number from the regex match
                inferred_value = match.group(1)
                # Check if this checkbox value has a decoded value in our dictionary
                if inferred_value in self.decoding_dict[code_list_oid]:
                    # Use the decoded (human-readable) value
                    return self.decoding_dict[code_list_oid][inferred_value]
                else:
                    # If no decoded value exists, use the raw value
                    return item_raw_value
            else:
                # If we can't extract a number from the column name, use the raw value
                return item_raw_value
        else:
            # For checkboxes, any value other than "1" means the box is not checked
            return None

    def _decode_standard_field(self, item_raw_value: str, code_list_oid: str) -> Any:
        """
        Decode a standard (non-checkbox) field value.

        Args:
            item_raw_value: Raw value from the XML
            code_list_oid: OID of the code list for decoding

        Returns:
            Decoded value or raw value if no decoding is available
        """
        # Check if the raw value exists in our decoding dictionary
        if item_raw_value in self.decoding_dict[code_list_oid]:
            # Use the decoded (human-readable) value
            return self.decoding_dict[code_list_oid][item_raw_value]
        else:
            # If no decoded value exists, use the raw value
            return item_raw_value

    def _process_item(self, item: Dict[str, Any], form_oid: str) -> Tuple[Optional[str], Optional[Any]]:
        """
        Process an individual item and handle its decoding.

        Args:
            item: Item data from the XML
            form_oid: OID of the form containing this item

        Returns:
            Tuple of (item_oid, processed_value), both can be None if item is not found
        """
        item_oid = item.get("@ItemOID", "")
        item_raw_value = item.get("@Value", "")

        # Check if this item OID is defined in the schema for this form
        # This ensures we only process items that were defined in the metadata
        if item_oid in self.schema_dict[form_oid]["columns"]:
            item_schema = self.schema_dict[form_oid]["columns"][item_oid]
            column_name = item_schema.get("column_name", "")
            column_type = item_schema.get("column_type", "")
            code_list_oid = item_schema.get("code_list_oid", None)

            # Check if this item has a code list and if that code list exists in our decoding dictionary
            # This determines if we need to decode the raw value to a human-readable value
            if code_list_oid and code_list_oid in self.decoding_dict:
                try:
                    # Special handling for checkbox fields which have a different decoding logic
                    if column_type == "checkbox":
                        value = self._decode_checkbox_field(column_name, item_raw_value, code_list_oid)
                    # For non-checkbox fields (e.g., radio buttons, dropdowns, etc.)
                    else:
                        value = self._decode_standard_field(item_raw_value, code_list_oid)
                # Catch any exceptions that might occur during decoding
                # This ensures the process doesn't fail if there's an issue with a single value
                except Exception as e:
                    logger.warning(f"Error decoding value for {item_oid}: {e}")
                    # Fall back to using the raw value if decoding fails
                    value = item_raw_value
            # If there's no code list or the code list isn't in our decoding dictionary
            # just use the raw value as is
            else:
                value = item_raw_value

            return item_oid, value

        return None, None

    def _add_record_to_table(self, table_name: str, record: Dict[str, Any]) -> None:
        """
        Add a record to the table_records dictionary.

        Args:
            table_name: Name of the table to add the record to
            record: Record data to add
        """
        # Check if we already have records for this table
        if table_name in self.table_records:
            # Get existing records and make a copy to avoid modifying the original list
            existing_records = self.table_records.get(table_name, []).copy()
        else:
            # If this is the first record for this table, initialize an empty list
            existing_records = []

        existing_records.append(record)
        self.table_records[table_name] = existing_records

    def process_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Process clinical data from the parsed XML data.

        Returns:
            Dictionary mapping table names to lists of records
        """
        study_oid = self.parsed_data["ODM"]["ClinicalData"].get("@StudyOID", "")
        subject_data_entries = self._ensure_list(self.parsed_data["ODM"]["ClinicalData"].get("SubjectData", []))

        for subject_data_entry in subject_data_entries:
            study_event_entries = self._ensure_list(subject_data_entry.get("StudyEventData", []))

            for study_event_entry in study_event_entries:
                form_data_entries = self._ensure_list(study_event_entry.get("FormData", []))

                for form_data_entry in form_data_entries:
                    form_oid = form_data_entry.get("@FormOID", "")

                    # Check if the form OID exists in our schema dictionary
                    # This ensures we only process forms that were defined in the metadata
                    if form_oid in self.schema_dict:
                        table_name = self.schema_dict[form_oid]["table_name"]

                        # Create a new record with common fields
                        new_record = self._create_base_record(
                            self.parsed_data["ODM"]["ClinicalData"].get("@StudyOID", ""),
                            subject_data_entry.get("@SubjectKey", ""),
                            study_event_entry.get("@redcap:UniqueEventName", "")
                        )

                        item_groups = self._ensure_list(form_data_entry.get("ItemGroupData", []))

                        for item_group in item_groups:
                            items = self._ensure_list(item_group.get("ItemData", []))

                            for item in items:
                                item_oid, value = self._process_item(item, form_oid)
                                if item_oid and value is not None:
                                    new_record[item_oid] = value

                        # Add the record to the appropriate table
                        self._add_record_to_table(table_name, new_record)

        return self.table_records

    def get_data_dataframes(self) -> Dict[str, pd.DataFrame]:
        """
        Convert the table records to pandas DataFrames.

        Returns:
            Dictionary mapping table names to DataFrames
        """
        dataframes = {}

        for table, records in self.table_records.items():
            dataframes[table] = pd.DataFrame.from_dict(records, orient='columns')

        return dataframes


class RedcapXmlParser:
    """
    Main class for parsing REDCap XML data.

    This class coordinates the parsing of REDCap XML data, including schema extraction,
    decoding, and data processing.
    """

    def __init__(self, file_path: str):
        """
        Initialize the RedcapXmlParser with a file path.

        Args:
            file_path: Path to the REDCap XML file
        """
        self.file_path = file_path
        self.parsed_data: Dict[str, Any] = {}
        self.schema_processor: Optional[SchemaProcessor] = None
        self.decoding_processor: Optional[DecodingProcessor] = None
        self.data_processor: Optional[DataProcessor] = None

        # Configure pandas display options
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option("display.max_colwidth", None)
        pd.set_option('display.width', 1000)
        pd.set_option('display.colheader_justify', 'center')
        pd.set_option('display.precision', 3)

    def parse_xml(self) -> Dict[str, Any]:
        """
        Parse the XML file.

        The file can be a local file or an S3 URL (s3://bucket/path/to/file).

        Returns:
            Dictionary containing parsed XML data
        """
        logger.info(f"Parsing XML file: {self.file_path}")

        try:
            # Check if the file is an S3 URL
            if self.file_path.startswith('s3://'):
                # Parse the S3 URL
                parsed_url = urllib.parse.urlparse(self.file_path)
                bucket_name = parsed_url.netloc
                object_key = parsed_url.path.lstrip('/')

                # Initialize the S3 client
                s3_client = boto3.client('s3')

                # Get the object from S3
                response = s3_client.get_object(Bucket=bucket_name, Key=object_key)

                # Read the content and decode to string
                xml_content = response['Body'].read().decode('utf-8')
            else:
                # Read from local file
                with open(self.file_path, 'r', encoding='utf-8') as file:
                    xml_content = file.read()

            # Parse the XML content
            self.parsed_data = xmltodict.parse(xml_content)

            return self.parsed_data
        except Exception as e:
            logger.error(f"Error parsing XML file: {e}")
            raise

    def process(self) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, str]], Dict[str, List[Dict[str, Any]]]]:
        """
        Process the REDCap XML data.

        Returns:
            Tuple containing schema dictionary, decoding dictionary, and table records
        """
        # Parse XML
        self.parse_xml()

        # Process schema
        logger.info("Processing schema information")
        self.schema_processor = SchemaProcessor(self.parsed_data)
        schema_dict = self.schema_processor.build_schema()

        # Process decoding
        logger.info("Processing decoding information")
        self.decoding_processor = DecodingProcessor(self.parsed_data)
        decoding_dict = self.decoding_processor.build_decoding_dict()

        # Process data
        logger.info("Processing clinical data")
        self.data_processor = DataProcessor(self.parsed_data, schema_dict, decoding_dict)
        table_records = self.data_processor.process_data()

        return schema_dict, decoding_dict, table_records

    def display_results(self) -> None:
        """
        Display the results of processing the REDCap XML data.
        """
        if not self.schema_processor or not self.decoding_processor or not self.data_processor:
            logger.warning("Results not available. Call process() first.")
            return

        # Display schema results
        schema_df = self.schema_processor.get_schema_dataframe()
        logger.info("Schema results:")
        print(schema_df)
        print("\n")

        # Display decoding results
        decoding_df = self.decoding_processor.get_decoding_dataframe()
        logger.info("Decoding results:")
        print(decoding_df)
        print("\n")

        # Display data results
        data_dfs = self.data_processor.get_data_dataframes()
        for table, df in data_dfs.items():
            logger.info(f"{table} records:")
            print(df)
            print("\n")

    def save_results(self, output_dir: str) -> None:
        """
        Save the results of processing the REDCap XML data to TSV files.

        Args:
            output_dir: Directory to save the TSV files
        """
        if not self.schema_processor or not self.decoding_processor or not self.data_processor:
            logger.warning("Results not available. Call process() first.")
            return

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Save schema results
        schema_df = self.schema_processor.get_schema_dataframe()
        schema_file = os.path.join(output_dir, "schema.tsv")
        schema_df.to_csv(schema_file, index=False, sep='\t')
        logger.info(f"Schema saved to {schema_file}")

        # Save decoding results
        decoding_df = self.decoding_processor.get_decoding_dataframe()
        decoding_file = os.path.join(output_dir, "decoding.tsv")
        decoding_df.to_csv(decoding_file, index=False, sep='\t')
        logger.info(f"Decoding saved to {decoding_file}")

        # Save data results
        data_dfs = self.data_processor.get_data_dataframes()
        for table, df in data_dfs.items():
            data_file = os.path.join(output_dir, f"{table}.tsv")
            df.to_csv(data_file, index=False, sep='\t', encoding='utf-8')
            logger.info(f"{table} records saved to {data_file}")


def main() -> None:
    """
    Main function to run the REDCap XML parser.

    This function parses a REDCap XML file (either local or from S3) and saves the results as TSV files.
    The TSV files include:
    - schema.tsv: Contains schema information for all tables
    - decoding.tsv: Contains decoding information for coded values
    - [table_name].tsv: One file for each table in the data
    """
    parser = argparse.ArgumentParser(description='Parse REDCap XML data')
    parser.add_argument('--file', '-f', type=str, required=True,
                        help='Path to the REDCap XML file. Can be a local file or an S3 URL (s3://bucket/path/to/file)')
    parser.add_argument('--output', '-o', type=str, default='output',
                        help='Directory to save output TSV files')

    args = parser.parse_args()

    # Create parser and process data
    redcap_parser = RedcapXmlParser(args.file)
    redcap_parser.process()

    # Save results to TSV files
    redcap_parser.save_results(args.output)


if __name__ == "__main__":
    main()
