import requests
import pandas as pd
from credentials import API_KEY


state = pd.read_csv("list_of_capitals.csv").dropna()
state_c = state.drop("State", axis=1)



def gen_state(state_cap):

    "Generates json weather data for each city (state capital) in Nigeria "
    for i in state_cap:
        re = requests.get(f"http://api.worldweatheronline.com/premium/v1/weather.ashx?key={API_KEY}&q={i}&format=json&num_of_days=1&date=today&fx=yes&cc=no&mca=no&includelocation=yes&tp=12&showlocaltime=yes&alerts=no&aqi=no")
        my_details = re.json()
        yield my_details
        #with open(f'cap{random.sample(range(1000), 1)}.json', 'w') as json_file:
        #    json.dump(my_details, json_file)
