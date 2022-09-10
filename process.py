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
    files = ['city', 'weather', 'astronomy']
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


def transform(files_data):
    upload_stream = StringIO()
    
    data = pd.merge(files_data['weather'], files_data['astronomy'].drop('date', axis=1), 
    left_on = 'city', right_on= 'city').drop_duplicates()
    
    
    data = data.merge(files_data['city'][['name', 'latitude','longitude']], right_on='name', left_on='city').drop_duplicates()

    dropables = ['date', 'maxtempF', 'avgtempF','mintempF','moonrise','totalSnow_cm', 'moonset','moon_illumination',
    "moon_phase", "uvIndex","name"]
    data['date'] = pd.to_datetime(data['date'])

    
    data['day'] = data['date'].apply(lambda dt: dt.day)
    data['day_name'] = data['date'].apply(lambda dt: dt.day_name())
    data['month'] = data['date'].apply(lambda dt: dt.month_name())
    data['year'] = data['date'].apply(lambda dt: dt.year)

    
    data = data.drop(dropables, axis=1)

    data.to_csv(upload_stream, index=False)

    
    return upload_stream.getvalue()



def upload_files(bucket="weather-ng"):
    files_data = {}
    for filename in files:
        
        file = streams[filename]
        file.seek(0)
        files_data[filename] = pd.read_csv(file)

    
    s3_file = transform(files_data)
    s3_resource = boto3.resource('s3')
    res = s3_resource.Object(bucket, 'weather_file.csv').put(Body=s3_file)