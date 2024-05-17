# Amazon Connect KVS Python Extraction

This repo is an extension of some previous work that I was looking at that [existing solution that AWS provides](https://github.com/amazon-connect/amazon-connect-salesforce-scv/tree/master/Solutions/VMX2-VoicemailExpress), and the [more recent top-level solution that AWS now recommended](https://github.com/amazon-connect/voicemail-express-amazon-connect). Namely, that it is a very good solution, however it uses mixed programming languages to achieve its required outcome, which can be a non-starter for some users of the platform. The NodeJS version also uses a number of packages that are provided in a Lambda layer, which makes it harder to use due to it.

## Acknowledgements

Normally this section would go later, but this needs to be stated up front. This piece of code would not be possible without some awesome work by [dcolcott](https://github.com/dcolcott) and then the follow up Pull Request by [micalgawlikaws](https://github.com/michalgawlikaws) on the [amazon-kinesis-video-streams-consumer-library-for-python repo](https://github.com/aws-samples/amazon-kinesis-video-streams-consumer-library-for-python/tree/main) that did majority of the heavy lifting in regards to processing the KVS data. There is little documentation and examples I could find in my searches as part of putting this together (in fact most seem to failback to using `ffmpeg`).

## About

This is largely a drop in replacement of the NodeJS solution, although I have some extra modifications from my codebase such as allowing to add a prefix to the S3 path, as well as partitioning based on time processed.
It is intended to be as readable as the original, with the catch being the nested subclasses can be harder to read rather than the `decoder` that is used in NodeJS.

If you want a rambling tale about how this solution eventuate, [you can find if on my blog](https://tech.yustin.nz/aws/amazon-connect/connect-voice-kvs-and-python-processing/)

Note that `vmx_to_s3.lambda_handler` is the primary entry point.
