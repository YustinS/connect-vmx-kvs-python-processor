"""
Voicemail processor for Python
"""

import os

# import sys
import base64
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3

from kvs_consumer import KvsPythonConsumerConnect

log = logging.getLogger()
# logging.basicConfig(
#     format="[%(name)s.%(funcName)s():%(lineno)d] - [%(levelname)s] - %(message)s",
#     stream=sys.stdout,
#     level=logging.INFO,
#     force=True
# )

# Boto3 clients ans related config
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SESSION = boto3.Session(region_name=AWS_REGION)
S3_CLIENT = SESSION.client("s3")

# Bucket configs. Assumption is BUCKET_NAME cannot be empty
BUCKET_NAME = os.environ["S3_VOICEMAIL_BUCKET"]
RECORDING_PATH = os.getenv("S3_RECORDING_PATH", "")
AUDIO_MIME_TYPE = "audio/x-wav"


def lambda_handler(event, context):
    """
    Fetch media from a specified Kinesis Video stream between the fragments
    """

    log.info(event)
    response_container = {}

    total_record_count = 0
    processed_record_count = 0

    for record in event["Records"]:
        total_record_count += 1

        # Extract the record data
        try:
            payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
            vm_record = json.loads(payload)
            current_contact_id = vm_record["ContactId"]
        except Exception as exc:
            log.error(exc)
            response_container[f"Record #{total_record_count} result"] = (
                "Failed to extract record and/or decode"
            )
            continue

        # Evaluate the VM recording flag
        try:
            vm_flag = vm_record["Attributes"].get("vm_flag", "99")

            if vm_flag == "1":
                log.info(
                    "Record #%i ContactID: %s - is a voicemail - begin processing",
                    total_record_count,
                    current_contact_id,
                )
            elif vm_flag == "0":
                response_container[f"Record #{total_record_count} result"] = (
                    f"ContactID: {current_contact_id} - IGNORE - voicemail already processed"
                )
                processed_record_count += 1
                continue
            else:
                response_container[f"Record #{total_record_count} result"] = (
                    f"ContactID: {current_contact_id} - IGNORE - voicemail flag not valid"
                )
                processed_record_count += 1
                continue
        except Exception as exc:
            log.error(exc)
            response_container[f"Record #{total_record_count} result"] = (
                f"ContactID: {current_contact_id} - IGNORE - An error occurred "
                "whilst evaluating Voicemail Flag"
            )
            continue

        # Extract the relevant KVS details for retrival
        try:
            stream_arn = vm_record["Recordings"][0]["Location"]
            stream_name = stream_arn[stream_arn.index("/") + 1 : stream_arn.rindex("/")]
            # Large value int. Trying to use float returns a scientific notation number
            fragment_start = int(vm_record["Recordings"][0]["FragmentStartNumber"])
            fragment_stop = int(vm_record["Recordings"][0]["FragmentStopNumber"])
        except Exception as exc:
            log.error(exc)
            response_container[f"Record #{total_record_count} result"] = (
                "Failed to extract KVS info"
            )
            continue

        # Prep the tagging data for the S3 Object write
        attribute_data = {}
        attribute_tag_container = ""
        try:
            attribute_data = vm_record.get("Attributes", {})
            for key, value in attribute_data.items():
                if key.startswith("vm_"):
                    attribute_tag_container += f"{key}={value}&"
            # Remove the trailing & symbol
            attribute_tag_container.rstrip("&")
        except Exception as exc:
            log.error(exc)
            response_container[f"Record #{total_record_count} result"] = (
                "Failed to extract vm tags"
            )
            continue

        log.info(
            "Attempting to process '%s' in stream '%s'", current_contact_id, stream_name
        )
        # Action the recording processing and subsequent upload
        try:
            processor = KvsPythonConsumerConnect(
                boto_session=SESSION,
                stream_name=stream_name,
                start_fragment=fragment_start,
                end_fragment=fragment_stop,
            )

            # Start the processing, waiting for completion of the activity
            processor.service_loop()

            # Grab the processed wav
            audio_file = processor.from_customer_audio

            # Generate the basepath in S3 using current information, accounting
            # for potential hour rollovers
            date = datetime.now(tz=ZoneInfo("UTC"))
            year = date.strftime("%Y")
            month = date.strftime("%m")
            day = date.strftime("%d")
            s3_base_path = f"{RECORDING_PATH}{year}/{month}/{day}/"

            s3_path = s3_base_path + current_contact_id + ".wav"

            log.info("Storing recording at '%s' in bucket '%s'", s3_path, BUCKET_NAME)
            # Create the S3 Put and complete it. We don't use the result,
            # simply want the outcome positive or negative
            put_response = S3_CLIENT.put_object(
                Body=audio_file.getvalue(), # type: ignore
                Bucket=BUCKET_NAME,
                Key=s3_path,
                ContentType=AUDIO_MIME_TYPE,
                Tagging=attribute_tag_container,
            )

            log.debug(put_response)

            processed_record_count += 1
        except Exception as exc:
            log.error(exc)
            response_container[f"Record #{total_record_count} result"] = (
                f"ContactID: {current_contact_id} - Failed to write audio to S3"
            )
            continue

    response = {
        "statusCode": 200,
        "body": {
            "status": f"Complete. Processed {processed_record_count} of {total_record_count} records.",
            "recordResults": response_container,
        },
    }

    log.info("Returning response ==> %s", response)

    return response
