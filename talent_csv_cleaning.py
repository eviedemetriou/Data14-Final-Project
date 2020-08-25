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
            self.cleaning_phone_numbers(df['phone_number'])

    def cleaning_phone_numbers(self, list_of_phones):        
        for phone in list_of_phones:
            if type(phone) is str:
                phone_filter = filter(str.isdigit, phone)
                clean_phone = "".join(phone_filter)
                clean_phone = f"+{clean_phone}"
                format_phone = phonenumbers.parse(clean_phone, 'GB')
                format_phone = phonenumbers.format_number(format_phone, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
                print(format_phone)
            else:
                print('No phone listed')


test = talent_csv()


