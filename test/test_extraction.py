import boto3
from ExtractionClass import ExtractFromS3

def test_extraction():
    extract = ExtractFromS3()
    assert len(extract) == 0


test_extraction()