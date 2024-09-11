# If Using azure then
#   pip install azure-identity
#   pip install azure-storage-blob
#   pip install wds-client

# If using google cloud then
#   pip install google-cloud-storage


# pip install python-dateutil
# pip install backoff
# pip install schema

# If getting azure token use:
#  pip install azure-identity azure-mgmt-resource
#     !az login --identity --allow-no-subscriptions
#     cli_token = !az account get-access-token | jq .accessToken
#     azure_token = cli_token[0].replace('"', '')

# To get gcp token if doing locally run:
#   pip install google-auth google-auth-httplib2 google-auth-oauthlib ?
#    gcloud auth application-default print-access-token
#

import json
import pytz
import os
import logging
import time
import re
import base64
import sys

import pandas as pd
import numpy as np

from urllib.parse import urlparse
from typing import Any, Optional
from datetime import datetime, date
from dateutil import parser
from dateutil.parser import ParserError


GCP = 'gcp'
AZURE = 'azure'












