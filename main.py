import os
import time
import requests
from time import sleep
from pathlib import Path
from config import MARKETS, INTERVAL, START_TIME

BASE_URL = 'https://fapi.binance.com/fapi/v1/continuousKlines?'

def main():
    for i in MARKETS:
        data = get_data(i, INTERVAL)

def get_data(market, interval):
    unix_time = int(time.time())
    print(f"Writing data for {market}")

    try:
        while True:
            req = requests.get(f"{BASE_URL}pair={market}&contractType=PERPETUAL&interval={interval}&startTime={START_TIME}")
            json = req.json()

            for i in json:
                string_to_write =f"{i[0]},{i[1]},{i[2]},{i[3]},{i[4]},{i[5]}\n"
                write_data(market, string_to_write)

            start_time = i[0]
            break

        while True:
            req = requests.get(f"{BASE_URL}pair={market}&contractType=PERPETUAL&interval={interval}&startTime={start_time}")
            json = req.json()

            k = 0
            for i in json:
                string_to_write =f"{i[0]},{i[1]},{i[2]},{i[3]},{i[4]},{i[5]}\n"
                write_data(market, string_to_write)
                k += 1

            if k < 150:
                # If k < 150 (arbitrary number less than 500) we've reached the current time and so can exit the loop.
                break

            start_time = i[0]
            sleep(1)

    except IndexError:
        print(f"IndexError: Error writing data for {market}")

    except:
        print(f"Uncaught Error: Error writing data for {market}")


def write_data(market, string_to_write):
    directory = f"data/{INTERVAL}/"
    file_name = f"{market}.csv"
    dir_plus_file_name = f"{directory}{file_name}"
    csv_header_string = 'TIME,OPEN,HIGH,LOW,CLOSE,VOLUME\n'

    if os.path.exists(dir_plus_file_name):
        with open(dir_plus_file_name, 'a') as f:
            f.write(string_to_write)

    else:
        Path(directory).mkdir(parents=True, exist_ok=True)
        with open(dir_plus_file_name, 'a+') as f:
            f.write(csv_header_string)
            f.write(string_to_write)


if __name__ == '__main__':
    main()
