import boto3
import pandas as pd
from io import BytesIO

def download_file(bucket="weather-ng"):
    session = session = boto3.Session(
        aws_access_key_id="AKIA5QCLBCXCN73S5X7N", 
        aws_secret_access_key="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")

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
    left_on = 'city', right_on= 'city').merge(files['city.csv'][['name', 'latitude','longitude','country']], right_on='name', left_on='city').drop_duplicates()
    dropables = ['date','maxtempF', 'avgtempF','mintempF','moonrise','totalSnow_cm', 'moonset','moon_illumination',
    "moon_phase", "uvIndex","name"]
    data['date'] = pd.to_datetime(data['date'])

    data['day'] = data['date'].apply(lambda dt: dt.day)
    data['day_name'] = data['date'].apply(lambda dt: dt.day_name())
    data['month'] = data['date'].apply(lambda dt: dt.month_name())
    data['year'] = data['date'].apply(lambda dt: dt.year)

    data = data.drop(dropables, axis=1)

    return data


files = download_file()
weather_file = transform(files)

print(weather_file.head())