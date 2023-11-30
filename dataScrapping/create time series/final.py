import os
from createTimeSeries import getYearlyDataForDistricts, mergeThroughYears, modifyDataForCSV, writeInCSV



DIRECTORY_PATH = '~/Desktop/CGWB/yearly/'



## DONT CHANGE BELOW

def getYears(path):
    items = os.listdir(DIRECTORY_PATH)
    directories = [int(item) for item in items if os.path.isdir(os.path.join(DIRECTORY_PATH, item)) and item[0] != '.']
    directories.sort()
    return directories

years = getYears(DIRECTORY_PATH)

def getStates(year):
    path = DIRECTORY_PATH + str(year)+'/'
    items = os.listdir(path)
    directories = [item for item in items if os.path.isdir(os.path.join(path, item)) ]
    directories.sort()
    return directories



states = getStates(years[-1])

def getDistricts(state, year):
    path = DIRECTORY_PATH + str(year)+'/'+str(state)+'/'
    items = os.listdir(path)
    directories = [item for item in items if os.path.isdir(os.path.join(path, item)) ]
    directories.sort()
    return directories



year = 2023
for state in states:
    print(f"starting state: {state}")
    districts = getDistricts(state, year)
    for district in districts:
        data = mergeThroughYears(state, district, years[0], years[-1])
        data = modifyDataForCSV(data)
        writeInCSV(data, f"/Users/manisarthak/Desktop/CGWB/timeSeries/{state}_{district}.csv")