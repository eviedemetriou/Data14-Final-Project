import boto3
from ExtractionClass import ExtractFromS3

def test_extraction():
    test = ExtractFromS3()
    # see if the lists beforehand are empty
    assert len(test.talent_csv_list) == 0
    assert len(test.talent_json_list) == 0
    assert len(test.talent_txt_list) == 0
    assert len(test.academy_csv_list) == 0
    # Run the get_data method to flood the lists with rows
    test.get_data()
    # See if the length of each list is greater than zero
    assert len(test.talent_csv_list) > 0
    assert len(test.talent_json_list) > 0
    assert len(test.talent_txt_list) > 0
    assert len(test.academy_csv_list) > 0
