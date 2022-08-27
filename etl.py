
import pandas as pd
from data_api import gen_state
from process import ParseFile, create_filestreams, load_file, upload_files
from data_api import gen_state

def process():

    state = pd.read_csv("list_of_capitals.csv").dropna()
    capitals = state['Capital'].values


    # Extract
    weather_data_gen = iter(gen_state(capitals))
    create_filestreams.has_been_called = False
    while True:
        
        try:
            jsondata = next(weather_data_gen)

            # Transform
            parser = ParseFile(json_handler=jsondata)
            data = {}

            city = parser.parse_area()
            weather, astronomy, hourly  = parser.parse_weather()

            

            data['city'] = city.dict()
            data['weather'] = weather.dict()
            data['astronomy'] = astronomy.dict()
            data['hourly'] = hourly

            # Load to s3

            if create_filestreams.has_been_called == False:
                create_filestreams(data=data)

            load_file(data=data)

        except StopIteration:
            print("Loading files completed!\nUploading files to aws s3 bucket...")
            upload_files()
            print('Uploaded')
            break





# Execute
process()
