from s3_project.classes.talent_csv_cleaning import TalentCsv
import os
test = TalentCsv()

def test_file_created():
    # Test to see if a file is created if the email does not contain an '@' symbol
    test_name = 'John Van Doe'
    test.flag_name(test_name)
    assert os.path.exists('C:/Users/sunny/Data14-Final-Project/s3_project/tests/monthly_applicant_names_edgecases.txt')

    os.remove('C:/Users/sunny/Data14-Final-Project/s3_project/tests/monthly_applicant_names_edgecases.txt')
