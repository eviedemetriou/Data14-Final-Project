from s3_project.classes.talent_csv_cleaning import TalentCsv
test_name = TalentCsv()

def test_first_name():
    assert test_name.splitting_first_names('John Doe') == 'John'