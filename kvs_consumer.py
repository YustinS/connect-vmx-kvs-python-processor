# https://github.com/aws-samples/amazon-kinesis-video-streams-consumer-library-for-python/blob/main/kvs_consumer_library_example.py

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0.

"""
Class definition of Kinesis Video Streams Consumer designed around Connect Voicemail.
This takes provided work and modifies it into a more general callable version, with 
options left to extend further.

Biggest question mark remains around large file sizes. Since data is stored in
memory it may become large quickly, so instead chunking may be required
"""

import io
import wave
import time
import logging
from amazon_kinesis_video_consumer_library.kinesis_video_streams_parser import (
    KvsConsumerLibrary,
)
from amazon_kinesis_video_consumer_library.kinesis_video_fragment_processor import (
    KvsFragementProcessor,
)

# Config the logger.
log = logging.getLogger(__name__)

class KvsPythonConsumerConnect:
    """
    Class for KVS Consumption for Connect data
    """

    def __init__(self, boto_session, stream_name, start_fragment, end_fragment):
        """
        Initialize the KVS clients as needed. The KVS Comsumer Library intentionally
        the KVS clients or the various media API calls. These have individual authentication
        configuration and a variety of other user defined settings so we keep them here
        in the users application logic for configurability.

        The KvsConsumerLibrary sits above these and parses responses from GetMedia and
        GetMediaForFragmentList into MKV fragments and provides convenience functions to further
        process, save and extract the data within each given frame.
        """

        # Create shared instance of KvsFragementProcessor
        self.kvs_fragment_processor = KvsFragementProcessor()

        # Variables initiated that move the state through
        self.stream_name = stream_name
        self.start_fragment = start_fragment
        self.end_fragment = end_fragment

        # Attributes used in processing
        self.last_good_fragment_tags = None
        self.past_end_fragment = False
        self.from_audio_fragments = bytearray()
        self.to_audio_fragments = bytearray()

        # Final results we want to use elsewhere
        self.to_customer_audio = None
        self.from_customer_audio = None

        # Init the KVS Service Client and get the accounts KVS service endpoint
        log.info("Initializing Amazon Kinesis Video client....")
        # Attach session specific configuration (such as the authentication pattern)
        self.session = boto_session
        self.kvs_client = self.session.client("kinesisvideo")

    ####################################################
    # Main process loop
    def service_loop(self):
        """
        Primary function loop that we call to await completion of processing.
        Will initiate the lookups, handle the fragments arriving, then ensure that
        the data has finished processing fully
        """

        ####################################################
        # Start an instance of the KvsConsumerLibrary reading in a Kinesis Video Stream

        # Get the KVS Endpoint for the GetMedia Call for this stream
        log.info(
            "Getting KVS GetMedia Endpoint for stream: %s ........", self.stream_name
        )
        get_media_endpoint = self._get_data_endpoint(self.stream_name, "GET_MEDIA")

        # Get the KVS Media client for the GetMedia API call
        log.info(
            "Initializing KVS Media client for stream: %s........", self.stream_name
        )
        kvs_media_client = self.session.client(
            "kinesis-video-media", endpoint_url=get_media_endpoint
        )

        # Make a KVS GetMedia API call with the desired KVS stream and
        # StartSelector type and time bounding.
        log.info(
            "Requesting KVS GetMedia Response for stream: %s........", self.stream_name
        )
        get_media_response = kvs_media_client.get_media(
            StreamName=self.stream_name,
            StartSelector={
                "StartSelectorType": "FRAGMENT_NUMBER",
                "AfterFragmentNumber": str(self.start_fragment),
            },
        )

        # Initialize an instance of the KvsConsumerLibrary, provide the GetMedia
        # response and the required call-backs
        log.info("Starting KvsConsumerLibrary for stream: %s........", self.stream_name)
        stream_consumer = KvsConsumerLibrary(
            self.stream_name,
            get_media_response,
            self.on_fragment_arrived,
            self.on_stream_read_complete,
            self.on_stream_read_exception,
        )

        # Start the instance of KvsConsumerLibrary, any matching fragments will begin
        # arriving in the on_fragment_arrived callback
        stream_consumer.start()

        # Here can hold the process up by waiting for the KvsConsumerLibrary thread to
        # finish (may never finish for live streaming fragments)
        while not self.past_end_fragment:
            time.sleep(1)

        log.info("Finished processing")

    ####################################################
    # KVS Consumer Library call-backs

    def on_fragment_arrived(
        self, stream_name, fragment_bytes, fragment_dom, fragment_receive_duration
    ):
        """
        This is the callback for the KvsConsumerLibrary to send MKV fragments as
        they are received from a stream being processed.
        The KvsConsumerLibrary returns the received fragment as raw bytes and a
        DOM like structure containing the fragments meta data.

        In this solution this simply parses the fragment for the relevant data
        and stores it against the Class for further consumption. Note that the 
        check around being over the end fragment is not generally used, but is
        provided as a guard function.

        ### Parameters:

            **stream_name**: str
                Name of the stream as set when the KvsConsumerLibrary thread
                triggering this callback was initiated.
                Use this to identify a fragment when multiple streams are read
                from different instances of KvsConsumerLibrary to this callback.

            **fragment_bytes**: bytearray
                A ByteArray with raw bytes from exactly one fragment. Can be save
                or processed to access individual frames

            **fragment_dom**: mkv_fragment_doc: ebmlite.core.Document <ebmlite.core.MatroskaDocument>
                A DOM like structure of the parsed fragment providing searchable
                list of EBML elements and MetaData in the Fragment

            **fragment_receive_duration**: float
                The time in seconds that the fragment took for the streaming
                data to be received and processed.

        """

        try:
            # Log the arrival of a fragment.
            # use stream_name to identify fragments where multiple instances of the
            # KvsConsumerLibrary are running on different streams.
            # log.info(
            #    "Fragment Received on Stream: %s", stream_name
            # )

            # Print the fragment receive and processing duration as measured by the
            # KvsConsumerLibrary
            log.info(
                "Fragment Receive and Processing Duration: %s Secs",
                fragment_receive_duration,
            )

            # Get the fragment tags and save in local parameter.
            self.last_good_fragment_tags = (
                self.kvs_fragment_processor.get_fragment_tags(fragment_dom)
            )

            current_fragment = int(
                self.last_good_fragment_tags["AWS_KINESISVIDEO_FRAGMENT_NUMBER"]
            )

            if int(current_fragment) > int(self.end_fragment):
                log.info("Pass final timestamp. Ending invocation")
                self.past_end_fragment = True
            else:
                # Checks for data in the FROM_CUSTOMER channel
                from_data = self.kvs_fragment_processor.save_connect_fragment_audio_track_from_customer(
                    fragment_dom
                )
                if from_data:
                    self.from_audio_fragments.extend(from_data)

                # Checks for data in the TO_CUSTOMER channel
                to_data = self.kvs_fragment_processor.save_connect_fragment_audio_track_to_customer(
                    fragment_dom
                )
                if to_data:
                    self.to_audio_fragments.extend(to_data)

        except Exception as err:
            log.error("on_fragment_arrived Error: %s", err)

    def on_stream_read_complete(self, stream_name):
        """
        This callback is triggered by the KvsConsumerLibrary when a stream has no more fragments
        available.
        This represents a graceful exit of the KvsConsumerLibrary thread.

        A stream will reach the end of the available fragments if the StreamSelector applied some
        time or fragment bounding on the media request or if requesting a live steam and the
        producer stopped sending more fragments.
        """

        # Convert the tracks to WAV ready for immediate usage in the calling stream.
        # This could instead be left as an activity for the caller, however in this case
        # we known exactly how this will be used
        try:
            if len(self.to_audio_fragments) > 0:
                self.to_customer_audio = self.convert_track_to_wav(
                    self.to_audio_fragments
                )

            if len(self.from_audio_fragments) > 0:
                self.from_customer_audio = self.convert_track_to_wav(
                    self.from_audio_fragments
                )

        except Exception as exc:
            log.error(exc)
        finally:
            # Ensure the end flag is appropriately set so that the service_loop()
            # exits cleanly and returns to the calling flow in synchronous usage,
            # or can be evaluated in async
            self.past_end_fragment = True

    def on_stream_read_exception(self, stream_name, error):
        """
        This callback is triggered by an exception in the KvsConsumerLibrary reading a stream.

        For example, to process use the last good fragment number from self.last_good_fragment_tags
        to restart the stream from that point in time with the example stream selector provided
        below.

        Alternatively, just handle the failed stream as per your application logic requirements.

        ### Parameters:

            **stream_name**: str
                Name of the stream as set when the KvsConsumerLibrary thread triggering this
                callback was initiated.
                Use this to identify a fragment when multiple streams are read from different
                instances of KvsConsumerLibrary to this callback.

            **error**: err / exception
                The Exception obje tvthat was thrown to trigger this callback.

        """

        # Can choose to restart the KvsConsumerLibrary thread at the last received
        # fragment with below example StartSelector
        # StartSelector={
        #    'StartSelectorType': 'FRAGMENT_NUMBER',
        #    'AfterFragmentNumber': self.last_good_fragment_tags['AWS_KINESISVIDEO_CONTINUATION_TOKEN'],
        # }

        # Here we just log the error
        log.error(
            "ERROR: Exception on read stream: %s\n Fragment Tags:%s\nError Message:%s",
            stream_name,
            self.last_good_fragment_tags,
            error,
        )

    ####################################################
    # KVS Helpers
    def _get_data_endpoint(self, stream_name, api_name):
        """
        Convenience method to get the KVS client endpoint for specific API calls.
        """
        response = self.kvs_client.get_data_endpoint(
            StreamName=stream_name, APIName=api_name
        )
        return response["DataEndpoint"]

    def convert_track_to_wav(self, track_bytearray):
        """
        This function converts a track bytearray to a wav file.
        """

        file_wav = io.BytesIO()
        with wave.open(file_wav, "wb") as f:
            f.setnchannels(1)
            f.setframerate(8000)
            f.setsampwidth(2)
            f.writeframes(track_bytearray)
        return file_wav

    def save_connect_fragment_audio_track_as_wav(self, byte_array, file_name_path):
        """
        Save the provided fragment_dom as wav file on local disk. Unused but
        Provided as a reference
        ### Parameters:
            audio_bytes
            file_name_path: Str
                Local file path / name to save the wav file to.
        """
        fragment_wav = self.convert_track_to_wav(byte_array)
        with open(file_name_path, "wb") as f:
            f.write(fragment_wav.getvalue())
