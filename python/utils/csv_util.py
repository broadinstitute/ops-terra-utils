import csv
import logging
from typing import Optional, Sequence


class Csv:
    def __init__(self, file_path: str, delimiter: str = '\t'):
        """
        Initialize the Csv class.

        Args:
            file_path (str): The path to the CSV file.
            delimiter (str, optional): The delimiter to use in the CSV file. Defaults to '\t'.
        """
        self.delimiter = delimiter
        self.file_path = file_path

    def create_tsv_from_list_of_dicts(self, list_of_dicts: list[dict], header_list: Optional[list[str]] = None) -> str:
        """
        Create a TSV file from a list of dictionaries.

        Args:
            list_of_dicts (list[dict]): The list of dictionaries to write to the TSV file.
            header_list (Optional[list[str]], optional): The list of headers to use in the TSV file.
                If provided output columns will be in same order as list. Defaults to None.

        Returns:
            str: The path to the created TSV file.
        """
        # Create one flat unique list by doing list comprehension where it loops
        # through twice to make it flat and transform to set and back to list
        # to make it unique
        if not header_list:
            header_list = sorted(
                list(
                    set(
                        [
                            header_list
                            for d in list_of_dicts
                            for header_list in d.keys()
                        ]
                    )
                )
            )
        logging.info(f'Creating {self.file_path}')
        with open(self.file_path, 'w') as f:
            writer = csv.DictWriter(
                f, fieldnames=header_list, delimiter='\t', quotechar="'", extrasaction='ignore')
            writer.writeheader()
            for d in list_of_dicts:
                writer.writerow(d)
        return self.file_path

    def create_tsv_from_list_of_lists(self, list_of_lists: list[list]) -> str:
        """
        Create a TSV file from a list of lists.

        Args:
            list_of_lists (list[list]): The list of lists to write to the TSV file.

        Returns:
            str: The path to the created TSV file.
        """
        logging.info(f'Creating {self.file_path}')
        with open(self.file_path, 'w') as f:
            for list_of_data in list_of_lists:
                # Make sure all entries are strings
                str_only_list = [str(entry) for entry in list_of_data]
                f.write(self.delimiter.join(str_only_list) + '\n')
        return self.file_path

    def create_list_of_dicts_from_tsv_with_no_headers(self, headers_list: list[str]) -> list[dict]:
        """
        Create a list of dictionaries from a TSV file with no headers.

        Args:
            headers_list (list[str]): The list of headers to use for the TSV file.

        Returns:
            list[dict]: The list of dictionaries created from the TSV file.
        """
        with open(self.file_path, 'r') as f:
            reader = csv.DictReader(
                f, delimiter=self.delimiter, fieldnames=headers_list)
            return [row for row in reader]

    def get_header_order_from_tsv(self) -> Optional[Sequence[str]]:
        """
        Get the header order from a TSV file.

        Returns:
            list[str]: The list of headers in the TSV file.
        """
        with open(self.file_path, 'r') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter, skipinitialspace=True)
            return reader.fieldnames

    def create_list_of_dicts_from_tsv(
            self, expected_headers: Optional[list[str]] = None,
            allow_extra_headers: bool = False
    ) -> list[dict]:
        """
        Create a list of dictionaries from a TSV file.

        Args:
            expected_headers (Optional[list[str]], optional): The list of expected headers. If provided
                will check that all headers are present in the TSV file. Defaults to None.
            allow_extra_headers (bool, optional): Whether to allow extra headers in the TSV file.
                Only used if expected_headers is provided. Defaults to False.

        Returns:
            list[dict]: The list of dictionaries created from the TSV file.

        Raises:
            ValueError: If the expected headers are not found in the TSV file.
        """
        with open(self.file_path) as f:
            dict_reader = csv.DictReader(
                f, delimiter=self.delimiter, skipinitialspace=True)
            if expected_headers:
                match = True
                tsv_headers = dict_reader.fieldnames
                extra_headers = set(tsv_headers) - set(expected_headers)  # type: ignore[arg-type]
                missing_headers = set(expected_headers) - set(tsv_headers)  # type: ignore[arg-type]
                if extra_headers:
                    extra_string = ','.join(extra_headers)
                    logging.warning(
                        f"Extra headers found in tsv: {extra_string}")
                    if not allow_extra_headers:
                        match = False
                if missing_headers:
                    missing_string = ','.join(missing_headers)
                    logging.error(
                        f"Missing expected headers: {missing_string}")
                    match = False
                if not match:
                    raise ValueError(
                        f"Expected headers not in {self.file_path}")
            return [
                {
                    k: v
                    for k, v in row.items()
                }
                for row in dict_reader
            ]
