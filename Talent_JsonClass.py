from ExtractionClass import ExtractFromS3
import boto3
import json
from datetime import datetime
from pprint import pprint


class ApplicantInfoClean(ExtractFromS3):

    def __init__(self):
        super().__init__()
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.bucket_list = self.s3_client.list_buckets()
        self.bucket_name = 'data14-engineering-project'
        self.bucket = self.s3_resource.Bucket(self.bucket_name)
        self.contents = self.bucket.objects.all()
        self.talent_json_list = []
        self.get_json_files_in_bucket()
        self.iterate()

    def get_json_files_in_bucket(self):
        for objects in self.contents:
            suffix = '.json'
            objects_key = objects.key
            if objects_key.endswith(suffix):
                if objects_key.startswith('Talent'):
                    self.talent_json_list.append(objects_key)
                    # print(objects_key)

    def iterate(self):
        dict = {}
        for file in self.talent_json_list:
            self.s3_object = self.s3_client.get_object(Bucket=self.bucket_name, Key=file)
            self.strbody = self.s3_object['Body'].read()
            self.object_dict = json.loads(self.strbody)
            self.split_first_name_and_surname()
            self.time_format()
            self.boolean_values()
            dict[file] = self.object_dict
        # pprint(dict)
        # print(len(dict))

    def split_first_name_and_surname(self):
        name_list = self.object_dict['name'].split(' ')
        if len(name_list) > 2:
            self.append_to_txt_file()
            # self.object_dict['first_name'] = " ".join(name_list[:-1])
            # self.object_dict['last_name'] = name_list[-1]
            return self.object_dict.keys
        elif len(name_list) == 2:
            self.object_dict['first_name'] = name_list[0]
            self.object_dict['last_name'] = name_list[-1]
            self.object_dict.pop('name')
            return None

    def append_to_txt_file(self):
        with open("applicant_info_edgecases.txt", "a") as ai:
            ai.writelines(f"{self.object_dict.keys}\n")

    def time_format(self):
        date = self.object_dict['date']
        date = date.replace('//', '/')
        self.object_dict['date'] = datetime.strptime(date, '%d/%M/%Y').strftime('%Y/%M/%d')

    def boolean_values(self):
        if self.object_dict['result'] == 'Fail' or 'fail':
            self.object_dict['result'] = False
        elif self.object_dict['result'] == 'Pass' or 'pass':
            self.object_dict['result'] = True
        if self.object_dict['self_development'] == 'No' or 'no':
            self.object_dict['self_development'] = False
        elif self.object_dict['self_development'] == 'Yes' or 'yes':
            self.object_dict['self_development'] = True
        if self.object_dict['financial_support_self'] == 'No' or 'no':
            self.object_dict['financial_support_self'] = False
        elif self.object_dict['financial_support_self'] == 'Yes' or 'yes':
            self.object_dict['financial_support_self'] = True
        if self.object_dict['geo_flex'] == 'No' or 'no':
            self.object_dict['geo_flex'] = False
        elif self.object_dict['geo_flex'] == 'Yes' or 'yes':
            self.object_dict['geo_flex'] = True


juxhen = ApplicantInfoClean()
