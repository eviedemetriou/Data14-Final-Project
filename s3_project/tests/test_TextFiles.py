from s3_project.classes.cleaning_txt import TextFiles

test = TextFiles()

def test_iterate_txt():
    assert len(test.file_contents) > 0
    assert len(test.file_contents) == len(test.talent_txt_list)



#def test_split_name_results():
