import boto3
import pandas as pd
from ExtractionClass import ExtractFromS3

class Academy(ExtractFromS3):

    def __init__(self):
        super().__init__()
        self.df_list = []
        self.read_object()
        self.split_trainee_names('sparta_scores_issues.txt')
        self.split_trainer_names()
        self.rearrange()


    def read_object(self):
        # Read the body of each object and append the contents to a list as dataframes
        for obj in self.academy_csv_list:
            s3_object = self.s3_client.get_object(
                Bucket= self.bucket_name,
                Key= obj)
            df = pd.read_csv(s3_object['Body'])
            df['course_name'] = obj.split('_')[0] +'_'+ obj.split('_')[1]  # Make a new column with the course name
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
                if len(df['first_name']) >=3:
                    self.append_to_txt_file(row['name'])
        self.df_list = new_df_list



    def append_to_txt_file(self,file):
        with open("sparta_scores_issues.txt", "w") as text_file:
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

            column_names = ["first_name","last_name","trainer_first_name","trainer_last_name",'course_name','course_start_date']
            for week in course_weeks:
                for behaviour in behaviours:
                    column_names.append(f'{behaviour}_W{week}')
            df = df[column_names]
            new_df_list.append(df)
        self.df_list = new_df_list


test = Academy()
print(test.df_list)
#print(test.rearrange())


