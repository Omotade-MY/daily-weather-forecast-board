import json
import csv
import os
import pandas as pd
from datetime import datetime
from typing import Optional

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
            
            
            