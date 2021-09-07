import logging
import boto3
import json
logging.basicConfig(level=logging.INFO)
def handle(event,context):
    #a = event.query["bucket"]
    """handle a request to the function
    Args:
        req (str): request body
    """
    #import logging
    #logging.basicConfig(level=logging.INFO)
    import boto3
    s3 = boto3.resource('s3',aws_access_key_id='AKIAVNHZW6RT25SFJXSM',
                    aws_secret_access_key= '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')
    obj = s3.Object('snorkel-data', 'unlabelleddata/config.json')
    body = obj.get()['Body'].read()
    body_json = json.loads(body)

    logging.info("yash Bucket  =  "+ event.query['bucket'])
    logging.info("yash filename =  "+ event.query['filename'])
    logging.info("yash bucket data \n" + str(body_json["steps"]["common"]["dist"]["upper_threshold"] ))
    return "Hi World: Flask testing :  "+ event.query['bucket']

