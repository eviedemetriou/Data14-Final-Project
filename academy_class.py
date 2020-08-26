import boto3
import pandas as pd
import datetime
import math
from ExtractionClass import ExtractFromS3

class Academy(ExtractFromS3):

    def __init__(self):
        super().__init__()
        self.df_list = []
        self.column_names = []
        self.read_object()
        self.split_trainee_names('sparta_scores_issues.txt')
        self.split_trainer_names()
        self.rearrange()
        self.check_scores()


    def read_object(self):
        # Read the body of each object and append the contents to a list as dataframes
        for obj in self.academy_csv_list:
            s3_object = self.s3_client.get_object(
                Bucket= self.bucket_name,
                Key= obj)
            df = pd.read_csv(s3_object['Body'])
            course_name_split = obj.split('_')[0] +'_'+ obj.split('_')[1]
            df['course_name'] = course_name_split.split('/')[1]  # Make a new column with the course name
            df['course_start_date'] = obj.split('_')[2].split('.')[0]   # Make a new column with the course start date
            self.df_list.append(df)


    def split_trainee_names(self,file):
        # Separate the first and last names of applicants
        new_df_list = []
        for df in self.df_list:
            trainee_split = df['name'].str.split()
            df['first_name'] = trainee_split.str[:-1]
            df['last_name'] = trainee_split.str[-1]
            new_df_list.append(df)
            for index, row in df.iterrows():
                if len(row['first_name']) >=2:
                    self.append_to_txt_file(row['name'])
                    self.append_to_txt_file(row['course_name'])
        self.df_list = new_df_list


    def append_to_txt_file(self,file):
        with open("sparta_scores_issues.txt", "a") as text_file:
            text_file.writelines(f"{file}\n")


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
            behaviours = ["Analytic","Independent","Determined",
                          "Professional","Studious","Imaginative"]
            course_duration = (len(list(df.columns)) - 6)/ len(behaviours)
            course_weeks = list(range(1,int(course_duration) + 1))

            self.column_names = ["first_name", "last_name", "trainer_first_name", "trainer_last_name", 'course_name',
                            'course_start_date']
            for week in course_weeks:
                for behaviour in behaviours:
                    self.column_names.append(f'{behaviour}_W{week}')
            df = df[self.column_names]
            new_df_list.append(df)
        self.df_list = new_df_list


    def check_scores(self):
        new_df_list = []
        for df in self.df_list:
            for col in range(6, len(self.column_names)):
                for score in df.iloc[:, col]:
                    if not math.isnan(score):
                        score = int(score)
            new_df_list.append(df)
        self.df_list = new_df_list

test = Academy()
#print(test.check_scores())
print(test.df_list)
#print(test.rearrange())


