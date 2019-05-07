import json as json
import http.client, urllib.request, urllib.parse, urllib.error, base64
import pandas as pd
from time import sleep

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


def retrieve_data_sets(api_key):

    headers = {'token': api_key}

    try:
        conn = http.client.HTTPSConnection('www.ncdc.noaa.gov')
   
        conn.request("GET", "/cdo-web/api/v2/datasets?limit=100" ,None,  headers)
        response = conn.getresponse()
        data = response.read()
        jdata = json.loads(data.decode("utf-8") )

        df = pd.DataFrame(data=jdata['results']) 
    
        conn.close()

        return df
    except Exception as e:
        print(e)
        return pd.DataFrame()


def retrieve_data_categories(api_key):

    headers = {'token': api_key}

    try:
        conn = http.client.HTTPSConnection('www.ncdc.noaa.gov')
   
        conn.request("GET", "/cdo-web/api/v2/datacategories?limit=100" ,None,  headers)
        response = conn.getresponse()
        data = response.read()
        jdata = json.loads(data.decode("utf-8") )

        df = pd.DataFrame(data=jdata['results']) 
    
        conn.close()

        return df
    except Exception as e:
        print(e)
        return pd.DataFrame()

def retrieve_data_types_by_categoryid(api_key, datacategoryid= 'TEMP'):

    headers = {'token': api_key}

    params = urllib.parse.urlencode({
      'datacategoryid':datacategoryid,
      'limit':100
    })

    try:
        conn = http.client.HTTPSConnection('www.ncdc.noaa.gov')
   
        conn.request("GET", "/cdo-web/api/v2/datatypes?%s" % params ,None,  headers)
        response = conn.getresponse()
        data = response.read()
        jdata = json.loads(data.decode("utf-8") )

        df = pd.DataFrame(data=jdata['results']) 
    
        conn.close()

        return df
    except Exception as e:
        print(e)
        return pd.DataFrame()





def retrieve_weather(api_key,start,stop,fips,datasetid='GHCND',datatype='TAVG' ):
  
  offset = 0 #staring offset
  remaining = 100 #default, gets overwritten
  cnt = 1 #used as limit multiplier
  limit = 1000 #bring back full 1000 limit from NOAA API
  retry = 0
  
  print("Start: " + start + " Stop: " + stop )
  print(fips)
  print(datasetid)
  print(datatype)

  # NOAA API limits records to 1000, loop until all records for call are returned
  while remaining > 0:

    headers = {
      'token': api_key
    }

    params = urllib.parse.urlencode({
      'datasetid':datasetid,
      'locationid': fips,
      'startdate':start,
      'enddate': stop,
      'limit':limit,
      'units':'standard',
      'datatypeid':datatype,
      'offset':offset
    })
    
    try:
        conn = http.client.HTTPSConnection('www.ncdc.noaa.gov')
   
        conn.request("GET", "/cdo-web/api/v2/data?%s" % params,None,  headers)
        response = conn.getresponse()
        data = response.read()
        jdata = json.loads(data.decode("utf-8") )
    
        conn.close()
    except Exception as e:
        print(e)
    
    if 'metadata' in jdata.keys():
        
        
        #How many records are in the result set
        recCount = int(jdata['metadata']['resultset']['count'])  

        #offset by returned records (anything less than full limit will be last loop)
        offset += limit

        #calc remaining records for debugging
        remaining = recCount - (limit*cnt)

        print(remaining)

        df = pd.DataFrame(data=jdata['results'])   


        if cnt <=1:
            tempdata = df

        else:        
            tempdata = tempdata.append(df)


        cnt+=1

        sleep(2)
    else:
        print('no metadata')
        if retry > 2:
            return pd.DataFrame()
        else:
            retry += 1
            sleep(4)
      
  return tempdata
    



