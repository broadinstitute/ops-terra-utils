import csv
import logging
from typing import Optional


class Csv:
    def __init__(self, file_path: str, delimiter: str = '\t'):
        self.delimiter = delimiter
        self.file_path = file_path

    def create_tsv_from_list_of_dicts(self, list_of_dicts: list[dict], header_list: Optional[list[str]] = None) -> str:
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
                f, fieldnames=header_list, delimiter='\t', extrasaction='ignore')
            writer.writeheader()
            for d in list_of_dicts:
                writer.writerow(d)
        return self.file_path

    def create_tsv_from_list_of_lists(self, list_of_lists: list[list]) -> str:
        logging.info(f'Creating {self.file_path}')
        with open(self.file_path, 'w') as f:
            for list_of_data in list_of_lists:
                # Make sure all entries are strings
                str_only_list = [str(entry) for entry in list_of_data]
                f.write(self.delimiter.join(str_only_list) + '\n')
        return self.file_path

    def create_list_of_dicts_from_tsv_with_no_headers(self, headers_list: list[str]) -> list[dict]:
        with open(self.file_path, 'r') as f:
            reader = csv.DictReader(
                f, delimiter=self.delimiter, fieldnames=headers_list)
            return [row for row in reader]

    def create_list_of_dicts_from_tsv(self, expected_headers: Optional[list[str]] = None) -> list[dict]:
        """if expected headers then will validate they match headers in file"""
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
                    logging.error(
                        f"Extra headers found in tsv: {extra_string}")
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
