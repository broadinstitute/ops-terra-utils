import httplib2
import pytz
import logging
import requests
import os
from typing import Optional, Union
from datetime import datetime, timedelta

from . import GCP, AZURE  # import from __init__.py


class Token:
    def __init__(self, cloud: Optional[str] = None, token_file: Optional[str] = None) -> None:
        self.cloud = cloud
        self.expiry: Optional[datetime] = None
        self.token_string: Optional[str] = ""
        # If provided with a file just use the contents of file
        if token_file:
            self.token_file = token_file
            with open(self.token_file) as f:
                self.token_string = f.read().rstrip()
        else:
            self.token_file = ""
            # If not provided with a file must authenticate with either google or azure python libraries
            if self.cloud == GCP:
                # Only import libraries if needed
                from oauth2client.client import GoogleCredentials
                self.credentials = GoogleCredentials.get_application_default()
                self.credentials = self.credentials.create_scoped(
                    [
                        "https://www.googleapis.com/auth/userinfo.profile",
                        "https://www.googleapis.com/auth/userinfo.email",
                        "https://www.googleapis.com/auth/devstorage.full_control"
                    ]
                )
            elif self.cloud == AZURE:
                # Only import libraries if needed
                from azure.identity import DefaultAzureCredential
                self.credentials = DefaultAzureCredential()
                self.az_token = self.credentials.get_token(
                    "https://management.azure.com/.default")
            else:
                raise ValueError(f"Cloud {self.cloud} not supported. Must be {GCP} or {AZURE}")

    def _get_gcp_token(self) -> Union[str, None]:
        # Refresh token if it has not been set or if it is expired or close to expiry
        if not self.token_string or not self.expiry or self.expiry < datetime.now(pytz.UTC) + timedelta(minutes=5):
            http = httplib2.Http()
            self.credentials.refresh(http)
            self.token_string = self.credentials.get_access_token().access_token
            # Set expiry to use UTC since google uses that timezone
            self.expiry = self.credentials.token_expiry.replace(tzinfo=pytz.UTC)  # type: ignore[union-attr]
            # Convert expiry time to EST for logging
            est_expiry = self.expiry.astimezone(pytz.timezone("US/Eastern"))  # type: ignore[union-attr]
            logging.info(f"New token expires at {est_expiry} EST")
        return self.token_string

    def _get_az_token(self) -> Union[str, None]:
        # This is not working... Should also check about timezones once it does work
        if not self.token_string or not self.expiry or self.expiry < datetime.now() - timedelta(minutes=10):
            self.az_token = self.credentials.get_token("https://management.azure.com/.default")
            self.token_string = self.az_token.token
            self.expiry = datetime.fromtimestamp(self.az_token.expires_on)
        return self.token_string

    def _get_sa_token(self):
        if not self.token_string or not self.expiry or self.expiry < datetime.now(pytz.UTC) + timedelta(minutes=5):
            SCOPES = ['https://www.googleapis.com/auth/userinfo.profile', 'https://www.googleapis.com/auth/userinfo.email']
            url = f"http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token?scopes={','.join(SCOPES)}"
            token_response = requests.get(url, headers={'Metadata-Flavor': 'Google'})
            self.token_string = token_response.json()['access_token']
        return token_response.json()['access_token']

    def get_token(self) -> Union[str, None]:
        # If token file provided then always return contents
        if self.token_file:
            return self.token_string
        elif self.cloud == GCP:
            # detect if this is running as a cloud run job
            if os.getenv("CLOUD_RUN_JOB"):
                return self._get_sa_token()
            else:
                return self._get_gcp_token()
        else:
            return self._get_az_token()
