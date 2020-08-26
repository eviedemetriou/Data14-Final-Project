from s3_project.classes.extraction_class import ExtractFromS3
import boto3
import json
from datetime import datetime


class ApplicantInfoClean(ExtractFromS3):

    def __init__(self):
        super().__init__()
        self.clean_files()

    def clean_files(self):
        # This method iterates through each file, and applies the cleaning methods to each file.
        # This method also appends the cleaned files to a dictionary.
        object_dict = {}
        talent_json_dict = {}
        for file in self.talent_json_list:
            s3_object = self.s3_client.get_object(Bucket=self.bucket_name, Key=file)
            body = s3_object['Body'].read()
            object_dict = json.loads(body)
            if len(object_dict['name'].split(' ')) > 2:
                self.append_file(file)
            self.split_names(object_dict)
            self.date_format(object_dict)
            self.boolean_values(object_dict)
            talent_json_dict[file] = object_dict
        return object_dict

    def split_names(self, object_dict):
        # This method splits name into first_name and last_name,
        # if there's more than 2 names, every name but the last goes into the first_name column,
        # and the ones with more than 2 names get appended to a text file.
        name_list = object_dict['name'].split(' ')
        if len(name_list) > 2:
            object_dict['first_name'] = " ".join(name_list[:-1])
            object_dict['last_name'] = name_list[-1]
            object_dict.pop('name')
        elif len(name_list) == 2:
            object_dict['first_name'] = name_list[0]
            object_dict['last_name'] = name_list[-1]
            object_dict.pop('name')
        return [object_dict['first_name'], object_dict['last_name']]

    def append_file(self, file):
        # This method appends a text file.
        with open("applicant_info_edgecases.txt", "a") as ai:
            ai.writelines(f"{file}\n")

    def date_format(self, object_dict):
        # This method cleans the date column
        date = object_dict['date']
        date = date.replace('//', '/')
        object_dict['date'] = datetime.strptime(date, '%d/%M/%Y').strftime('%Y/%M/%d')
        return object_dict['date']

    def boolean_values(self, object_dict):
        # This method changes result, self_dev, financial_support and geo_flex to boolean values.
        list_of_booleans = [object_dict['result'], object_dict['self_development'],
                            object_dict['financial_support_self'], object_dict['geo_flex']]
        list_of_booleans = list(map(lambda x: True if x == 'Yes' or x == 'Pass' else False, list_of_booleans))
        object_dict['result'] = list_of_booleans[0]
        object_dict['self_development'] = list_of_booleans[1]
        object_dict['financial_support_self'] = list_of_booleans[2]
        object_dict['geo_flex'] = list_of_booleans[3]
        return [object_dict['result'], object_dict['self_development'], object_dict['financial_support_self'], object_dict['geo_flex']]
