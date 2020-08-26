import boto3
from pprint import pprint
from ExtractionClass.py import ExtractFromS3

class TextFiles(ExtractFromS3):

    def __init__(self):
        super().__init__()
        self.txt_file_names = []
        self.get_txt_files()
        self.contents = []
        self.iterate_txt()
        self.results =[]
        self.split_name_results()
        self.final_list = []
        self.get_scores()

    def get_txt_files(self):
        for objects in self.bucket_contents:
            suffix = '.txt'
            objects_key = objects.key
            if objects_key.endswith(suffix):
                if objects_key.startswith('Talent'):
                    self.txt_file_names.append(objects_key)

    def iterate_txt(self):
        for i in self.txt_file_names:
            s3_object = self.s3_client.get_object(Bucket=self.bucket_name, Key=i)
            body = s3_object['Body'].read()
            strbody = body.decode('utf-8').splitlines()
            self.contents.append({'date': strbody[0], 'location': strbody[1], 'results': strbody[3:]})

    def split_name_results(self):
        for item in self.contents:
            split = str(item['results']).strip(' Psychometrics: ').strip('Presentation:').split()
            psyc_index = split.index('Psychometrics:')
            self.results.append({'first_name': str(split[0:psyc_index-2]).strip('["[\ ').strip("'\'").strip('"]')
                                    ,'last_name': split[psyc_index-2] ,'date': item["date"], 'location': item["location"]
                                    , 'psyc': split[psyc_index+1].strip(','), 'pres': split[psyc_index+3].strip("',")})

    def get_scores(self):
        for item in self.results:
            psyc = item['psyc'].split('/')
            pres = item['pres'].split('/')
            self.final_list.append({'first_name': item['first_name'], 'last_name': item['last_name']
                                       , 'date': item['date'], 'psychometrics': int(psyc[0])
                                       , 'psychometric_max': int(psyc[1]), 'presentation': int(pres[0])
                                       , 'presentation_max': int(pres[1].strip('"'))})
        print(list(self.final_list))

























print(TextFiles())