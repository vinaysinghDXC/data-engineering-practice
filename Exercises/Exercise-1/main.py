import requests
import os
import aiohttp
import asyncio
import zipfile
from concurrent.futures import ThreadPoolExecutor

# Define the list of file URLs
download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

# Create the 'downloads' directory if it doesn't exist
if not os.path.exists("downloads"):
    os.makedirs("downloads")

# Step 1: Download files asynchronously using aiohttp
async def download_file_async(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            filename = os.path.basename(url)
            zip_path = os.path.join("downloads", filename)
            with open(zip_path, "wb") as f:
                f.write(await response.read())
            return zip_path

async def main_async():
    tasks = [download_file_async(url) for url in download_uris]
    await asyncio.gather(*tasks)

# Step 2: Extract CSV files from ZIP archives
def extract_csv_from_zip(zip_path):
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        for file_info in zip_ref.infolist():
            if file_info.filename.lower().endswith(".csv"):
                csv_filename = os.path.join("downloads", file_info.filename)
                zip_ref.extract(file_info, "downloads")
                print(f"Extracted CSV from {zip_path}: {csv_filename}")

# Step 3: Download using ThreadPoolExecutor for additional concurrency
def download_files_threadpool():
    with ThreadPoolExecutor(max_workers=len(download_uris)) as executor:
        zip_paths = list(executor.map(download_file_async, download_uris))
        for zip_path in zip_paths:
            extract_csv_from_zip(zip_path)
            os.remove(zip_path)  # Delete the ZIP file

# Run the code
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main_async())
    download_files_threadpool()
