from s3_project.classes.extraction_class import ExtractFromS3
import boto3
import json
from datetime import datetime


class ApplicantInfoClean(ExtractFromS3):
    def __init__(self):
        super().__init__()
        self.iterate()

    # This method iterates through each file, and applies the cleaning methods to each file.
    # This method also appends the cleaned files to a dictionary.
    def iterate(self):
        object_dict = {}
        talent_json_dict = {}
        for file in self.talent_json_list:
            s3_object = self.s3_client.get_object(Bucket=self.bucket_name, Key=file)
            body = s3_object['Body'].read()
            object_dict = json.loads(body)
            self.split_first_name_and_surname(file, object_dict)
            self.time_format(object_dict)
            self.boolean_values(object_dict)
            talent_json_dict[file] = object_dict
        return object_dict

    # This method splits name into first_name and last_name,
    # if there's more than 2 names, every name but the last goes into the first_name column,
    # and the ones with more than 2 names get appended to a text file.
    def split_first_name_and_surname(self, file, object_dict):
        name_list = object_dict['name'].split(' ')
        if len(name_list) > 2:
            object_dict['first_name'] = " ".join(name_list[:-1])
            object_dict['last_name'] = name_list[-1]
            object_dict.pop('name')
            self.append_to_txt_file(file)
        elif len(name_list) == 2:
            object_dict['first_name'] = name_list[0]
            object_dict['last_name'] = name_list[-1]
            object_dict.pop('name')

    # This method appends a text file.
    def append_to_txt_file(self, file):
        with open("applicant_info_edgecases.txt", "a") as ai:
            ai.writelines(f"{file}\n")

    # This method cleans the date column
    def time_format(self, object_dict):
        date = object_dict['date']
        date = date.replace('//', '/')
        object_dict['date'] = datetime.strptime(date, '%d/%M/%Y').strftime('%Y/%M/%d')
        return object_dict['date']

    # This method changes result, self_dev, financial_support and geo_flex to boolean values.
    def boolean_values(self, object_dict):
        if object_dict['result'] == 'Fail' or 'fail':
            object_dict['result'] = False
        elif object_dict['result'] == 'Pass' or 'pass':
            object_dict['result'] = True
        if object_dict['self_development'] == 'No' or 'no':
            object_dict['self_development'] = False
        elif object_dict['self_development'] == 'Yes' or 'yes':
            object_dict['self_development'] = True
        if object_dict['financial_support_self'] == 'No' or 'no':
            object_dict['financial_support_self'] = False
        elif object_dict['financial_support_self'] == 'Yes' or 'yes':
            object_dict['financial_support_self'] = True
        if object_dict['geo_flex'] == 'No' or 'no':
            object_dict['geo_flex'] = False
        elif object_dict['geo_flex'] == 'Yes' or 'yes':
            object_dict['geo_flex'] = True
