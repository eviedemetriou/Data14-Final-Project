import boto3
from ExtractionClass import ExtractFromS3

def test_extraction():
    test = ExtractFromS3()
    assert len(test.talent_csv_list) == 0
    test.get_data()
    assert len(test.talent_csv_list) > 0
    assert len(test.talent_json_list) > 0
    assert len(test.talent_txt_list) > 0
    assert len(test.academy_csv_list) > 0
