import boto3


class ExtractFromS3:
    # initialisation with lists to hold objects obtainted from methods
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.bucket_list = self.s3_client.list_buckets()
        self.bucket_name = 'data14-engineering-project'
        self.bucket = self.s3_resource.Bucket(self.bucket_name)
        self.contents = self.bucket.objects.all()
        self.academy_csv_list = []
        self.talent_csv_list = []
        self.talent_json_list = []
        self.talent_txt_list = []

    # extract csvs from academy method
    def extract_academy_csv(self):
        for objects in self.contents:
            suffix = '.csv'
            objects_key = objects.key
            if objects_key.endswith(suffix):
                if objects_key.startswith('Academy'):
                    self.academy_csv_list.append(objects_key)

    # extract csvs from talent method
    def extract_talent_csv(self):
        for objects in self.contents:
            suffix = '.csv'
            objects_key = objects.key
            if objects_key.endswith(suffix):
                if objects_key.startswith('Talent'):
                    self.talent_csv_list.append(objects_key)

    # extract json files from talent method
    def extract_talent_json(self):
        for objects in self.contents:
            suffix = '.json'
            objects_key = objects.key
            if objects_key.endswith(suffix):
                if objects_key.startswith('Talent'):
                    self.talent_json_list.append(objects_key)

    # extract txt files from talent method
    def extract_talent_txt(self):
        for objects in self.contents:
            suffix = '.txt'
            objects_key = objects.key
            if objects_key.endswith(suffix):
                if objects_key.startswith('Talent'):
                    self.talent_txt_list.append(objects_key)

