import json as json
import http.client, urllib.request, urllib.parse, urllib.error, base64
import pandas as pd

# Return United State locations
def retrieve_all_us_states(api_key):

    headers = {'token': api_key}

    try:
        conn = http.client.HTTPSConnection('www.ncdc.noaa.gov')
   
        conn.request("GET", "/cdo-web/api/v2/locations?locationcategoryid=ST&limit=52" ,None,  headers)
        response = conn.getresponse()
        data = response.read()
        jdata = json.loads(data.decode("utf-8") )

        df = pd.DataFrame(data=jdata['results']) 
    
        conn.close()

        return df
    except Exception as e:
        print(e)
        return pd.DataFrame()


