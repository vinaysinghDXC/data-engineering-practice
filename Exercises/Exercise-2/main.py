import requests
from bs4 import BeautifulSoup
import pandas as pd

def scrape_weather_data():
    # Step 1: Web scraping to find the correct file
    url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the link corresponding to the specified timestamp
    target_timestamp = '2022-02-07 14:03'
    file_link = None
    for link in soup.find_all('a', href=True):
        if target_timestamp in link.text:
            file_link = link['href']
            break

    if not file_link:
        print(f"File not found for timestamp {target_timestamp}.")
        exit()

    # Step 2: Download the file
    base_url = 'https://www.ncei.noaa.gov'
    full_url = base_url + file_link
    filename = file_link.split('/')[-1]

    print(f"Downloading file: {filename} ...")
    response = requests.get(full_url)
    with open(filename, 'wb') as f:
        f.write(response.content)
    print(f"File downloaded successfully: {filename}")

    return filename

def analyze_weather_data(filename):
    # Step 3: Load data into Pandas
    df = pd.read_csv(filename)

    # Find record(s) with the highest HourlyDryBulbTemperature
    max_temp = df['HourlyDryBulbTemperature'].max()
    max_temp_records = df[df['HourlyDryBulbTemperature'] == max_temp]

    print(f"Records with highest HourlyDryBulbTemperature ({max_temp}°F):")
    print(max_temp_records)

if __name__ == "__main__":
    downloaded_file = scrape_weather_data()
    analyze_weather_data(downloaded_file)
