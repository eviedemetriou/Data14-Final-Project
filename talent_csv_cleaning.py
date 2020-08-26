import phonenumbers
from ExtractionClass import ExtractFromS3
import pandas as pd


class talent_csv(ExtractFromS3):
    def __init__(self):
        # Inherited Extraction class, ran method to get data
        super().__init__()
        super().get_data()
        self.running_cleaner_methods()

    def running_cleaner_methods(self):
        # Iterating through list of csv's, accessing the Body to enable cleaning
        for index in self.talent_csv_list:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=index)
            df = pd.read_csv(obj['Body'])
            df['phone_number'] = df['phone_number'].apply(self.cleaning_phone_numbers)
            df['first_name'] = df['name'].apply(self.splitting_first_names)
            df['last_name'] = df['name'].apply(self.splitting_last_names)
            print(df.head(10))

    def cleaning_phone_numbers(self, phone):
        if type(phone) is str:
            phone_filter = filter(str.isdigit, phone)
            clean_phone = "".join(phone_filter)
            format_phone = clean_phone[:2] + ' ' + clean_phone[2:5] + ' ' + clean_phone[5:8] + ' ' + clean_phone[8:]
            format_phone = f'+{format_phone}'
            return format_phone
        else:
            return phone

    def splitting_first_names(self, name):
        if type(name) is str:
            first_name = name.split()[1]
            # first_name_filter = filter(str.isalpha, first_name)
            # first_name = "".join(first_name_filter)
            return first_name
        else:
            return name

    def splitting_last_names(self, name):
        if type(name) is str:
            last_name = name.split()[-1]
            last_name_filter = filter(str.isalpha, last_name)
            last_name = "".join(last_name_filter)
            return last_name
        else:
            return name


test = talent_csv()


