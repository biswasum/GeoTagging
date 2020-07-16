# importing the requests library 
import requests 
import pandas as pd 
import time 

base_url = "http://10.xx6.x03.1x1:xxx/"
## MMI geocoding prod API 
endpoint = "advancedmaps/v1/geo_code"
URL = base_url + endpoint

# Importing  records in batches in a dataframe 
## Fetch the  Customer Address column  to fetch the geo points 

#Full_data = pd.read_csv("E:/GeoCode/casa_address/casa_address_3 - Copy.csv",squeeze = True, engine = 'python')
start = time.time()
Latitude = []
Longitude =[]

Original_df = pd.DataFrame(columns=['FINAL_ADDRESS','Lat','Lng'])
Original_df.to_csv("E:/GeoCode/Output/casa_address4_chunksize500.csv", index = False)

for j in pd.read_csv("E:/GeoCode/casa_address/casa_address_4.csv",usecols=['FINAL_ADDRESS'],chunksize= 500,engine ='python'):
    i = 0
    for i in range(len(j.index)):
        PARAMS = {'addr':j.iloc[i,0]}

        # sending Geo-service get request and saving the response as json object 

        r = requests.get(url = URL, params = PARAMS, headers={'lickey': 'c82899fb-c414-4b05-86f6-af267ea04a25'})
        data = r.json()

        # printing the output
        print("Row Id : ",j.index,"....",i)
        latitude = data['results'][0]['lat']
        longitude = data['results'][0]['lng']
        Latitude.append (latitude)
        Longitude.append (longitude)
        
    new_df= pd.DataFrame({'Lat': Latitude,'Lng': Longitude})
    final_df = pd.concat([j.reset_index(drop=True), new_df], axis=1)
    final_df.to_csv("E:/GeoCode/Output/casa_address4_chunksize500.csv",mode = 'a',index = False, header = False)
    
    # Clear the dataframes 
    Latitude = []
    Longitude =[]
    new_df= new_df.empty
    final_df=final_df.empty
    
end = time.time()
total_time = end - start
print ("Time elapsed since epoch is : ", total_time) 
  
    