import csv
import copy
import pandas as pd
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DIRECTORY_PATH = "~/Desktop/CGWB/yearly/" ## PATH WHERE CGWB DATA IS FETCHED !!
WRITE_PATH = "~/Desktop/CGWB/timeSeries/" ## path where data is to be written
START_YEAR = 1994
END_YEAR = 2023



def readFromExcel(state, district, year, season, directory_path = DIRECTORY_PATH):
    excel_path = directory_path + f"{year}/{state}/{district}/{district}_{state}_{year}_{season}.xlsx"
    print(excel_path)
    scam = []
    try:
        df = pd.read_excel(excel_path, header=None)
        scam = df.values.tolist()[1:]
    except Exception as e:
        print("Error --- \n", year, state, district, season)
        print(e)
    return scam


def getYearlyDataForDistricts(year, state, district):
    """
    Returns a dictionary containing the station_id as 
    key and a list of station_name, lat, long, water_level
    for each season as value.
    
    None means data not available for that well in that season
    """
    data = dict()
    temp_data = []
    for season in range(1, 5):
        scam = readFromExcel(state, district, year, season)
        print(year, state, district, season, 'done')
        # print(scam, end = "\n\n\n\n")
        temp_data.append(scam)
        for x in scam:
            station_id = x[1]
            station_name = x[0]
            strtion_latitude = x[3]
            station_longitude = x[4]
            water_level = x[2]
            if station_id not in data:
                data[station_id] = [station_name, strtion_latitude, station_longitude]
            elif data[station_id][0] !=  station_name:
                print("some big error in the website or data fetching !!   check data")
                print(year, state, district, season)
                
    for seasonally in temp_data:
        data_cpy = copy.deepcopy(data)
        for x in seasonally:
            station_id = x[1]
            water_level = x[2]
            if station_id in data:
                data[station_id].append(water_level)
                data_cpy.pop(station_id)
        for x in data_cpy:
            data[x].append(None)      
    print(year, state, district, 'done\n\n') 
    return data


def mergeThroughYears(state, district, start_year=2010, end_year=2012)   :
    data = dict()
    data['station_id'] = ['station_name', 'latitude', 'longitude']
    temp_data = []
    for year in range(start_year, end_year+1):
        yearly_data_dict = getYearlyDataForDistricts(year, state, district)
        temp_data.append(yearly_data_dict)
        for x in yearly_data_dict:
            station_id = x
            station_name = yearly_data_dict[x][0]
            strtion_latitude = yearly_data_dict[x][1]
            station_longitude = yearly_data_dict[x][2]
            if station_id not in data:
                data[station_id] = [station_name, strtion_latitude, station_longitude]
            elif data[station_id][0] !=  station_name:
                print("some big error in the website or data fetching !!   check data")
                print(year, state, district)
                break
    year = start_year
    for yearly in temp_data:
        data_cpy = copy.deepcopy(data)
        for season in range(1, 5):
            data['station_id'].append(f"{year}_season{season}")
        for x in yearly:
            station_id = x
            water_level = yearly[x][3:]
            season += 1
            if station_id in data:
                data[station_id] += water_level
                data_cpy.pop(station_id)
        for x in data_cpy:
            if x != 'station_id':
                data[x] += ([None]*4)
        year+=1
    print(state, district, f'done from {start_year} to {end_year}\n\n\n\n')        
    return data
       
       
def modifyDataForCSV(data):
    final_dict = dict()
    final_dict['station_id'] = []
    idx_dict = dict()
    i = 0
    for k in data['station_id']:
        idx_dict[i] = k
        i += 1
        final_dict[k] = []
    for k in data:
        if k == 'station_id':
            continue
        final_dict['station_id'].append(k)
        for i in range(len(data[k])):
            final_dict[idx_dict[i]].append(data[k][i])
    return final_dict
       


def writeInCSV(data, csv_file):
    with open(csv_file, 'w', newline='') as csvfile:
        fieldnames = data.keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write the header
        writer.writeheader()

        # Write the dict
        for i in range(len(data['station_id'])):
            row = {field: data[field][i] for field in fieldnames}
            writer.writerow(row)

    print(f'Data saved to {csv_file}')







if __name__ == "__main__":
    state = 'Madhya Pradesh'
    district = 'Guna'
    data = mergeThroughYears(state, district, START_YEAR, END_YEAR)
    data = modifyDataForCSV(data)
    writeInCSV(data, WRITE_PATH+f"{state}_{district}.csv")



