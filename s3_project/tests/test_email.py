from s3_project.classes.talent_csv_cleaning import TalentCsv
import os
test = TalentCsv()

def test_email_valid():
    # Test to see if the function returns an email if it does include '@'
    assert test.email_valid('something@gmail.com')


def test_file_created():
    # Test to see if a file is created if the email does not contain an '@' symbol
    test_email = 'somethinggmail.com'
    test.email_valid(test_email)
    assert os.path.exists('C:/Users/sunny/Data14-Final-Project/s3_project/tests/monthly_applicant_emails_edgecases.txt')

    os.remove('C:/Users/sunny/Data14-Final-Project/s3_project/tests/monthly_applicant_emails_edgecases.txt')
