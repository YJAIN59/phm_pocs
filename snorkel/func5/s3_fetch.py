import os
import logging 

logging.basicConfig(level=logging.INFO)
logging.info("Hi  : "+os.environ['HOME'])

os.environ["AWS_ACCESS_KEY"] = "AKIAVNHZW6RT25SFJXSM"
os.environ["AWS_SECRET_KEY"] = "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"


logging.info("os.environ[\"AWS_ACCESS_KEY\"] is "+os.environ["AWS_ACCESS_KEY"]+" os.environ[\"AWS_SECRET_KEY\"] is " + os.environ["AWS_SECRET_KEY"])
