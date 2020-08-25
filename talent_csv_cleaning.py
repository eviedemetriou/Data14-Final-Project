import boto3
from pprint import pprint
import pandas as pd


class talent_csv:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.bucket_name = 'data14-engineering-project'
        self.bucket = self.s3_resource.Bucket(self.bucket_name)
        self.contents = self.bucket.objects.all()
        self.csv_objects = []
        self.get_csv_object()
        self.phone_numbers()

    def get_csv_object(self):
        for objects in self.contents:
            suffix = '.csv'
            objects_key = objects.key
            if objects_key.endswith(suffix):
                if objects_key.startswith('Talent'):
                    self.csv_objects.append(objects_key)

    # def read_csv(self):
    #     for item in self.csv_objects:
    #         object = self.s3_client.get_object(
    #             Bucket = self.bucket_name,
    #             Key = item)
    #         df = pd.read_csv(object['Body'])
    #         return df.head(50)

    def phone_numbers(self):
        for index in self.csv_objects:
            obj = self.s3_client.get_object(
                Bucket = self.bucket_name,
                Key = index)
            df = pd.read_csv(obj['Body'])
            for phone in df['phone_number']:
                if type(phone) is str:
                    if '-' in phone:
                        phone.replace('-', ' ')
                    if '(' in phone:
                        phone.replace('(', '')
                    if ')' in phone:
                        phone.replace(')', '')
                    if '=' in phone:
                        phone.replace('=', '+')
                    print(phone)


    # def cleaning_phones(self, phone):
    #     correct = False
    #     while not correct:
    #         if '-' in phone:
    #             phone.replace('-', ' ')
    #         if '(' in phone:
    #             phone.strip('(')
    #         if ')' in phone:
    #             phone.strip(')')
    #         if '=' in phone:
    #             phone.replace('=', '+')
    #         else:
    #             correct = True
    #     return phone




test = talent_csv()
print(test.phone_numbers())
# s3_april_df = pd.read_csv(s3_talent_april['Body'])
# phone_numbers = s3_april_df.phone_number
# # print(phone_numbers)
#
#
# # def phone_format(phones):
# phone_list = []
# for phone in phone_numbers:
#     correct = False
#     while not correct:
#         if phone.startswith('='):
#             phone.replace('=', '+')
#         elif '-' in phone:
#             phone.replace('-', ' ')
#         elif '(' in phone:
#             phone.strip('(')
#         elif ')' in phone:
#             phone.strip(')')
#         else:
#             correct = True
#             phone_list.append(phone)
# print(phone_list)
# #
# #
# # phone_format(phone_numbers)