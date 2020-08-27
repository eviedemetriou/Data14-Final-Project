from s3_project.classes.talent_csv_cleaning import TalentCsv
test = TalentCsv()

def test_date_format():
    assert test.dob_formatting("26/07/1999") == "1999/07/26"