from s3_project.classes.applicant_info_class import ApplicantInfoClean
from s3_project.classes.cleaning_txt import TextFiles

instance_applicant_info_clean = ApplicantInfoClean()
instance_cleaning_txt = TextFiles()

print(instance_applicant_info_clean.create_dataframe(instance_applicant_info_clean.clean_files()))
print(instance_cleaning_txt.to_dataframe())

