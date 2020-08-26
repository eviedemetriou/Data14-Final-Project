import boto3
from ExtractionClass import ExtractFromS3
test = ExtractFromS3()

def test_extraction():
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

def test_check_files():
    test.get_data()
    # check lists actually contain the specific file types
    # Iterate through talent csv list and see if all files end in .csv
    for file in test.talent_csv_list:
        assert file.endswith('.csv')
    # Iterate through talent json list and see if all files end in .json
    for file in test.talent_json_list:
        assert file.endswith('.json')
    # Iterate through talent txt list and see if all files end in .txt
    for file in test.talent_txt_list:
        assert file.endswith('.txt')
    # Iterate through academy csv list and see if all files end in .csv
    for file in test.academy_csv_list:
        assert file.endswith('.csv')

def test_greater_than_one():
    test.get_data()
    # See if the length of the list isn't just one
    assert len(test.talent_csv_list) > 1
    assert len(test.talent_json_list) > 1
    assert len(test.talent_txt_list) > 1
    assert len(test.academy_csv_list) > 1
