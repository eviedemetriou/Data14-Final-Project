import boto3
import pandas as pd
from ExtractionClass import ExtractFromS3

class Academy(ExtractFromS3):

    def __init__(self):
        super().__init__()
        self.df_list = []
        self.get_data()
        self.read_object()
        self.split_applicant_names()
        self.split_trainer_names()
        self.rearrange()


    def read_object(self):
        # Read the body of each object and append the contents to a list as dataframes
        for obj in self.academy_csv_list:
            s3_object = self.s3_client.get_object(
                Bucket= self.bucket_name,
                Key= obj)
            df = pd.read_csv(s3_object['Body'])
            self.df_list.append(df)


    def split_applicant_names(self):
        # Separate the first and last names of applicants
        new_df_list = []
        for df in self.df_list:
            applicant_split = df['name'].str.split()
            df['first_name'] = applicant_split.str[:-1]
            df['last_name'] = applicant_split.str[-1]
            new_df_list.append(df)
        self.df_list = new_df_list


    def split_trainer_names(self):
        # Separate the first and last names of trainers
        new_df_list = []
        for df in self.df_list:
            trainer_split = df['trainer'].str.split()
            df['trainer_first_name'] = trainer_split.str[:-1]
            df['trainer_last_name'] = trainer_split.str[-1]
            new_df_list.append(df)
        self.df_list = new_df_list


    def rearrange(self):
        # Rearranging the order of the dataframes
        new_df_list = []
        for df in self.df_list:
            df = df[["first_name","last_name","trainer_first_name","trainer_last_name",
                               "Analytic_W1","Independent_W1","Determined_W1","Professional_W1","Studious_W1","Imaginative_W1",
                               "Analytic_W2","Independent_W2","Determined_W2","Professional_W2","Studious_W2","Imaginative_W2",
                               "Analytic_W3","Independent_W3","Determined_W3","Professional_W3","Studious_W3","Imaginative_W3",
                               "Analytic_W4","Independent_W4","Determined_W4","Professional_W4","Studious_W4","Imaginative_W4",
                               "Analytic_W5","Independent_W5","Determined_W5","Professional_W5","Studious_W5","Imaginative_W5",
                               "Analytic_W6","Independent_W6","Determined_W6","Professional_W6","Studious_W6","Imaginative_W6",
                               "Analytic_W7","Independent_W7","Determined_W7","Professional_W7","Studious_W7","Imaginative_W7",
                               "Analytic_W8","Independent_W8","Determined_W8","Professional_W8","Studious_W8","Imaginative_W8",
                               ]]
            new_df_list.append(df)
        self.df_list = new_df_list

test = Academy()
print(test.df_list)


