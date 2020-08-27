from s3_project.classes.applicant_info_class import talent_applicant_info
from s3_project.classes.cleaning_txt import talent_txt
from s3_project.classes.academy_class import academy_dataframe


print(talent_applicant_info.create_dataframe(talent_applicant_info.clean_files()))
print(talent_txt.to_dataframe())
print(academy_dataframe.cleaned_df)


