import requests
import pandas as pd
import random
import json



state = pd.read_csv("list_of_capitals.csv").dropna()
state_c = state.drop("State", axis=1)
state_c["Capital"]


def gen_state(state_cap):
    for i in state_cap:
        re = requests.get(f"http://api.worldweatheronline.com/premium/v1/weather.ashx?key=990fb081a1fd4ce886f201922221408&q={i}&format=json&num_of_days=1&date=today&fx=yes&cc=no&mca=no&includelocation=yes&tp=12&showlocaltime=yes&alerts=no&aqi=no")
        my_details = re.json()
        with open(f'cap{random.sample(range(1000), 1)}.json', 'w') as json_file:
            json.dump(my_details, json_file)


gen_state(state_c["Capital"].values)