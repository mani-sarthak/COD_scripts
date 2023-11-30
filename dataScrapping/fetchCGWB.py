import os
import time
from yearly import *

# Define the directory path
directory_path = "/Users/manisarthak/Desktop/cgwb_1/"

# year to fetch from before ie 2023 2022 2021 and so on till 1994
LAST_YEAR = 2023
LAST_STATE = None
LAST_DISTRICT = None




### try not to change below this line ###
log_file = "log.txt"
progress_file = "progress.txt"

if not os.path.exists(directory_path):
    os.makedirs(directory_path)


def log_timestamp(message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as log:
        log.write(f"{timestamp} - {message}\n")


if os.path.isfile(progress_file):
    with open(progress_file, 'r') as f:
        last_year = int(f.readline().strip())
        last_state = f.readline().strip()
        last_district = f.readline().strip()
else:
    last_year = LAST_YEAR
    last_state = LAST_STATE
    last_district = LAST_DISTRICT
    
    
log_timestamp(f"Starting from year {last_year}, state {last_state}, district {last_district}")

start_time = time.perf_counter()

for year in range(last_year, 1994, -1):
    log_timestamp(f"Started processing year {year}")
    dp_year = directory_path + str(year) + '/'

    if not os.path.exists(dp_year):
        os.makedirs(dp_year)

    states = getStates(year)

    if last_state:
        states = states[states.index(last_state):]

    for state in states:
        log_timestamp(f"Started processing state {state}")
        dp_state = dp_year + str(state) + '/'

        if not os.path.exists(dp_state):
            os.makedirs(dp_state)

        districts = getDistricts(state, year)

        if last_district:
            districts = districts[districts.index(last_district):]

        for district in districts:
            with open(progress_file, 'w') as f:
                f.write(str(year) + '\n')
                f.write(state + '\n')
                f.write(district + '\n')
            log_timestamp(f"Started processing district {district}")
            dp_district = dp_state + str(district) + '/'

            if not os.path.exists(dp_district):
                os.makedirs(dp_district)

            for season in range(5):
                data = getStationsInDistrict(state, district, year, season)
                data = addLatLong(data)
                create_excel(data, year, state, district, dp_district, season)

            log_timestamp(f"Finished processing district {district}")

        log_timestamp(f"Finished processing state {state}")
        last_district = None
    log_timestamp(f"Finished processing year {year}")
    last_state = None

    

log_timestamp("Script finished")
total_time = time.perf_counter() - start_time
log_timestamp(f"Total time taken: {total_time} seconds")
