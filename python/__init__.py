import os


if os.getenv("CLOUD_RUN_JOB"):
	import google.cloud.logging
	client = google.cloud.logging.Client()
	client.setup_logging()
