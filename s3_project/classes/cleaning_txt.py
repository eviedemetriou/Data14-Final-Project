import boto3
import pandas as pd
from s3_project.classes.ExtractionClass import ExtractFromS3
from datetime import datetime


class TextFiles(ExtractFromS3):

    def __init__(self):
        super().__init__()
        self.file_contents = []
        self.iterate_txt()
        self.results = []
        self.split_name_results()
        self.split_list = []
        self.get_scores()
        self.two_names_txt()
        self.final_list = []
        self.date_format()
        self.to_dataframe()

    #Gets the information from the body of the txt file
    def iterate_txt(self):
        for i in self.talent_txt_list:
            s3_object = self.s3_client.get_object(Bucket=self.bucket_name, Key=i)
            body = s3_object['Body'].read()
            strbody = body.decode('utf-8').splitlines()
            self.file_contents.append({'date': strbody[0], 'location': strbody[1], 'results': strbody[3:]})

    # splits the name and results string to first_name, last_name, psychometric, presentation
    def split_name_results(self):
        for item in self.file_contents:
            split = str(item['results']).strip(' Psychometrics: ').strip('Presentation:').split()
            psyc_index = split.index('Psychometrics:')
            self.results.append({'first_name': str(split[0:psyc_index - 2]), 'last_name': split[psyc_index - 2]
                                    , 'date': item["date"], 'location': item["location"]
                                    , 'psyc': split[psyc_index + 1].strip(','),
                                 'pres': split[psyc_index + 3].strip("',")})

    # splits the presentation and psychometric scores into score and max scores, also formats the name to title casing
    def get_scores(self):
        for item in self.results:
            psyc = item['psyc'].split('/')
            pres = item['pres'].split('/')
            name_filter = filter(lambda x: x.isalpha() or x.isspace(), item['first_name'])
            name_clean = "".join(name_filter)
            self.split_list.append({'first_name': name_clean.title(), 'last_name': item['last_name'].title()
                                       , 'date': item['date'], 'location': item['location'], 'psychometrics': int(psyc[0])
                                       , 'psychometric_max': int(psyc[1]), 'presentation': int(pres[0])
                                       , 'presentation_max': int(pres[1].strip('"'))})

    # append the 2 name names to a text file
    def two_names_txt(self):
        for name in self.split_list:
            if " " in list(name['first_name']):
                with open('../../sparta_days_txt_2names.txt', "a") as text_file:
                    text_file.writelines(f"{name['first_name']} {name['last_name']} in file: {name['date']}{name['location']}\n")

    # formats the date into YYYY/mm/dd format
    def date_format(self):
        for item in self.split_list:
            date = datetime.strptime(item['date'], '%A %d %B %Y').strftime('%Y/%m/%d')
            self.final_list.append({'first_name': item['first_name'], 'last_name': item['last_name'], 'date': date
                                    ,'location': item['location'], 'psychometrics': item['psychometrics']
                                    , 'psychometric_max': item['psychometric_max'],'presentation': item['presentation']
                                    , 'presentation_max': item['presentation_max']})

    # turns dictionary into a dataframe
    def to_dataframe(self):
        dataframe = pd.DataFrame(self.final_list)
        print(dataframe.dtypes)
        return dataframe









print(TextFiles())
