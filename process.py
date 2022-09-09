from webbrowser import get
import boto3
from io import StringIO, BytesIO
import os
import csv
import os
import pandas as pd
from datetime import datetime
from typing import Optional
from credentials import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

from models import Astronomy, City, Weather






class ParseFile:
    def __init__(self, json_handler: Optional[dict]):
        
        if type(json_handler) != dict:
            json_file = json_handler()
        else:
            json_file = json_handler
        
        self.json_data = json_file.get('data')
        self.time_zone = self.json_data['time_zone'][0]
        self.weather: Optional[Weather] = None
        self.astronomy: Optional[Astronomy] = None

    def parse_area(self) -> City:
        """
        ------------
        return type: dict
        ------------

        """

        area = self.json_data['nearest_area'][0]

        area_info = []
        for ar in area.items():
            ar = list(ar)
            if ar[0] == 'areaName':
                ar[0] = 'name'
            if type(ar[1]) == list:
                ar[1] = ar[1][0]['value']
            area_info.append(ar)
        
        area_info = dict(area_info)
        

        self.city = area_info['name']
        area_info['zone'] = self.time_zone['zone']
        area_info['utcOffset'] = self.time_zone['utcOffset']
        city = City.parse_obj(area_info)
        return city

    
    def parse_weather(self) -> tuple:
        if not (self.weather and self.astronomy):

            weather_dict = self.json_data.get('weather')[0]  
            weather_dict.pop('date')
            weather_dict['city'] = self.city
            weather_dict['date'] = self.time_zone['localtime']
            hourly = weather_dict.pop('hourly')[0]
            hourly['city'] = self.city
            hourly['time'] = self.time_zone['localtime']
            
            #weather_dict['hourly'] = hourly

            astronomy_dict = weather_dict.pop('astronomy')[0]
            astronomy = Astronomy.parse_obj(astronomy_dict)
            astronomy.city = self.city
            astronomy.date = self.time_zone['localtime']

            

            weather = Weather.parse_obj(weather_dict)
            self.weather = weather
            self.astronomy = astronomy
            self.hourly = hourly

        return self.weather, self.astronomy, self.hourly


def to_csv(filename: str, data):

    dirname = "./weather" + datetime.now().strftime("%Y-%m-%d-%h")
    if not os.path.isdir(dirname):
        os.mkdir(dirname)

    filepath = dirname+ "/" + filename
    if os.path.isfile(filepath):
        os.remove(filepath)

    with open(filepath, 'a+') as fp:
        writer = csv.DictWriter(fp, data)
        writer.writeheader()
        writer.writerow( data)



def create_filestreams(data):

    """Create file streams for city, wetaher, astronomy, and hourly"""

    
    create_filestreams.has_been_called = True

    global streams, writers, files
    streams = {}
    writers = {}
    files = ['city', 'weather', 'astronomy', 'hourly']
    for fl in files:

        streams[fl] = StringIO()
        writers[fl] = csv.DictWriter(streams[fl], data[fl])
        writers[fl].writeheader()

def load_file(data):
    for fl in files:
        writers[fl].writerow(data[fl])


def get_session():
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID, 
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    return session



def upload_files(bucket="weather-ng"):
    session = get_session()
    for filename in files:
        file = streams[filename].getvalue()
        s3_resource = session.resource('s3')
        res = s3_resource.Object(bucket, filename+'.csv').put(Body=file)
        #if res['ResponseMetadata']['HTTPStatusCode'] == 200:

def download_file(bucket="weather-ng"):
    session = get_session()
    s3 = session.client('s3')
    files = {}

    for file in s3.list_objects(Bucket=bucket)['Contents']:
        filename = file['Key']
        file_obj = s3.get_object(Bucket=bucket, Key=filename)
        file_stream = file_obj['Body'].read()
        file_data = pd.read_csv(BytesIO(file_stream))
        files[filename] = file_data
    return files    


def transform(files):
    data = pd.merge(files['weather.csv'], files['astronomy.csv'].drop('date', axis=1), 
    left_on = 'city', right_on= 'city').merge(files['city.csv'][['name', 'latitude','longitude',"country"]], right_on='name', left_on='city').drop_duplicates()
    dropables = ['date','maxtempF', 'avgtempF','mintempF','moonrise','totalSnow_cm', 'moonset','moon_illumination',
    "moon_phase", "uvIndex","name"]
    data['date'] = pd.to_datetime(data['date'])

    data['day'] = data['date'].apply(lambda dt: dt.day)
    data['day_name'] = data['date'].apply(lambda dt: dt.day_name())
    data['month'] = data['date'].apply(lambda dt: dt.month_name())
    data['year'] = data['date'].apply(lambda dt: dt.year)

    data = data.drop(dropables, axis=1)

    return data