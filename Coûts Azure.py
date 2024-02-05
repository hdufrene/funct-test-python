# %%
import requests as REQ
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import datetime, timedelta,date
from datetime import datetime, timedelta
from calendar import monthrange

from dateutil.relativedelta import relativedelta
# from typing import Dict, List, Optional, Tuple, Union
import json
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import matplotlib.font_manager as font_manager
import matplotlib.ticker as mticker
import matplotlib.cm as cm
from matplotlib.colors import ListedColormap, Normalize  
from matplotlib.cm import ScalarMappable
from matplotlib.colors import LinearSegmentedColormap
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

import time
import math
import pycountry
import plotly.graph_objects as go
import locale
import colorsys

from azure.identity import ClientSecretCredential
from azure.mgmt.costmanagement import CostManagementClient
from azure.mgmt.costmanagement.models import QueryDefinition,QueryDataset
from azure.mgmt.resource import SubscriptionClient
from azure.mgmt.resource import ResourceManagementClient

pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', 10)

%store -r resourcesAzure resourcesCoreModel
%store -r subscriptions lignesFacturationAzure
%store -r resourcesDevTest
# %store -r gains_decommissionnement 
# %store -r gains_rightsizing_sql_database gains_reservation_sql_database
# %store -r gains_rightsizing_app_service gains_reservation_app_service 
# %store -r gains_reservation_virtual_machines

%store -r lignesFacturationCoreModel resourcesCoreModel resourcesDevTest
%store -r gains_tot_app_service_Core_Model gains_tot_sql_database_CoreModel gains_tot_virtual_machines_CoreModel
%store -r azure_prices azure_partnumbers liste_partnumbers_dev_test

# %% [markdown]
# Collecte coûts

# %%
def get_country(liste_countries,latitude,longitude):
    
    if not latitude == None and not longitude==None:
        
        if type(liste_countries)==pd.core.frame.DataFrame and not liste_countries[(liste_countries["latitude"]==latitude)&(liste_countries["longitude"]==longitude)].empty:
            return (liste_countries[(liste_countries["latitude"]==latitude)&(liste_countries["longitude"]==longitude)]["country"].iloc[0],liste_countries)
        else:
            # u§rl = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={latitude}&lon={longitude}&accept-language=fr"
            
            
            url = "https://nominatim.openstreetmap.org/reverse"
            params = {
                "format": "json",
                "lat": latitude,
                "lon": longitude,
                "accept-language": "fr"
            }

            session = REQ.Session()
            retry = Retry(connect=3, backoff_factor=0.5)
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('https://', adapter)

            try:
                response = session.get(url, params=params)
                response.raise_for_status()  # Raise an HTTPError for bad responses
                data = response.json()
                # print(data)
            except REQ.exceptions.RequestException as e:
                print(f"Error: {e}")
                
    
    
            # response = REQ.get(url)
            # data = response.json()
            # print(data)
            if 'address' in data:
                if 'country' in data['address']:
                    country = data['address']['country'].encode('utf-8').decode('utf-8')
                    
                    if type(liste_countries)!=pd.core.frame.DataFrame:
                        liste_countries=pd.DataFrame.from_dict([{
                            "latitude":latitude,
                            "longitude":longitude,
                            "country":country
                        }])
                    else:
                        liste_countries=pd.concat([liste_countries,pd.DataFrame.from_dict([{
                            "latitude":latitude,
                            "longitude":longitude,
                            "country":country
                        }])],axis=0)
                    return (country.encode('utf-8').decode('utf-8'),liste_countries)
    return (None,liste_countries)


# def get_locations_subscription(subscriptionId):
#     url=f"https://management.azure.com/subscriptions/{subscriptionId}/locations?api-version=2020-01-01"
#     response=doRequest("GET",url,"")
#     if response==None:
#         return []
    
#     locations=response.json()["value"]
#     for i in range(len(locations)):
#         locations[i]["country"]=get_country(locations[i]["metadata"])
#         print("Locations ",subscriptionId," : ","{:.2%}".format(i/len(locations))," %")
#     return locations

# get_country(None,24.466667,54.366669)

def get_locations_subscription(subscription_id,liste_countries):
    
    credential =ClientSecretCredential(client_id="258d0525-a80c-4b29-bf60-9bd65ac5d418", client_secret="p3Q8Q~VOkEdF-hC-wfuNf.aUrwycYNiNpAibAakZ", tenant_id="f2460eca-756e-4a3f-bd14-d2a84590fc31")

    subscription_client = SubscriptionClient(credential)
    # subscription = next(subscription_client.subscriptions.list())
    locations =subscription_client.subscriptions.list_locations(subscription_id)
    locs=[]
    for location in locations : 
        
        country,liste_countries=get_country(liste_countries,location.metadata.latitude,location.metadata.longitude)
        
        if not country==None:
            loc = {
                "id":location.id,
                "name":location.name,
                "type":location.type,
                "regionalDisplayName":location.regional_display_name,
                "latitude":location.metadata.latitude,
                "longitude":location.metadata.longitude,
                "physicalLocation":location.metadata.physical_location,
                "regionCategory":location.metadata.region_category,
                "country":country
            }
            locs.append(loc)
            
    print("Abonnement",subscription_id,": récupération locations terminée")
    return (locs,liste_countries)

def get_current_month(date,limitedToDate):
    #date_format = "%Y-%m-%d"
    #date = datetime.strptime(d, date_format)
    first_day = date.replace(day=1)
    last_day = ((date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)).replace(hour=23).replace(minute=59).replace(second=59).replace(microsecond=1000000-1)
    if last_day>datetime.now() and limitedToDate:
        last_day=(datetime.now()- timedelta(days=1)).replace(hour=23).replace(minute=59).replace(second=59).replace(microsecond=1000000-1)
    return first_day, last_day

def generate_month_list(start_date,end_date):
    # date_format = "%Y-%m-%d"
    # date = datetime.strptime(start_date, date_format)
    # current_month = date.replace(day=1)
    # end_month = datetime.strptime(end_date, date_format)
    
    # month_list = []
    
    # while current_month <= end_month:
    #     month_list.append(current_month)
    #     current_month = current_month + timedelta(days=31)
    #     current_month = current_month.replace(day=1)
    
    # return month_list
    
    date_format = "%Y-%m-%d"
    start_date = datetime.strptime(start_date, date_format)
    current_month = start_date.replace(day=1)
    end_month = datetime.strptime(end_date, date_format)
    month_list = []
    while current_month <= end_month:
        month_list.append(current_month)
        current_month += relativedelta(months=1)
    return month_list


def get_count_days(date_str):
    # Conversion de la chaîne de date en objet datetime
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")

    # Obtention du nombre de jours dans le mois
    nombre_jours = monthrange(date_obj.year, date_obj.month)[1]

    return nombre_jours
# get_count_days("2023-02-01")

# %%
def get_subscriptions():
    credential =ClientSecretCredential(client_id="258d0525-a80c-4b29-bf60-9bd65ac5d418", client_secret="p3Q8Q~VOkEdF-hC-wfuNf.aUrwycYNiNpAibAakZ", tenant_id="f2460eca-756e-4a3f-bd14-d2a84590fc31")
    client = SubscriptionClient(credential)

    subscriptions = client.subscriptions.list()

    subscription_list = []
    liste_countries=None    
    path="C:\\Users\\hugo.dufrene\\OneDrive - Wavestone\\Documents\\FinOps\\Variables\\LocationCountriesAzure.csv"
    
    liste_countries=pd.read_csv(path)   
    
    for subscription in subscriptions:
        
        locations,liste_countries=get_locations_subscription(subscription.subscription_id,liste_countries)
         
        subscription_info = {
            "subscriptionId": subscription.subscription_id,
            "subscriptionName": subscription.display_name,
            "tenantId": subscription.tenant_id,
            "locations":locations
        }
        subscription_list.append(subscription_info)
        # break
    
    df_liste_countries=pd.DataFrame.from_dict(liste_countries)
    df_liste_countries.to_csv(path,index=False)
    
    return subscription_list

def get_infos_subscription(subscriptions, recherche):
    listeRecherche = []
    
    for sub in subscriptions:
        is_valid = False
        
        for key in sub.keys():
            if recherche == sub[key]:
                is_valid = True
        
        if is_valid:
            listeRecherche.append(sub)
    
    return listeRecherche

def get_infos_location(subscriptions, subscriptionId, search):
    if (
        search == "Unassigned"
        or search == "Unknown"
        or "Zone" in search
        or search == "Intercontinental"
        or len(search) == 0
        or search == "global"
    ):
        return {'id': None,
                'name': "Global",
                'type': 'Global',
                'regionalDisplayName': "Global",
                'latitude': None,
                'longitude': None,
                'physicalLocation': 'Global',
                'regionCategory': 'Global',
                'country': 'Global'}
    else:
        texte_recherches=""
        
        locations = []
        for sub in subscriptions:
            if sub["subscriptionId"] == subscriptionId:
                locations = sub["locations"]
                break
        # print("Recherche location",subscriptionId,search)
        # for loc in locations:
        recherche = search.replace("AP Southeast", "(Asia Pacific) Southeast Asia").replace("AP East", "(Asia Pacific) East Asia")
        recherche = recherche.lower().replace(" ", "")
        
        results = []
        results2 = []
        results3 = []

        for loc in locations:
            if (
                loc["id"].lower().replace(" ", "") == recherche
                or loc["name"].lower().replace(" ", "") == recherche
                or recherche in loc["regionalDisplayName"].lower().replace(" ", "")
            ):
                results.append(loc)
        # print("Recherche 1",recherche)
        texte_recherches+=recherche + " "

        # print("Result1")
        # print(results)
        # print()
        
        if len(results) == 0:
            splittedSearch = search.split(" ")
            if len(splittedSearch) == 2:
                recherche = search.split(" ")[1] + " " + search.split(" ")[0]
            elif len(splittedSearch) == 3:
                if search.split(" ")[2].isdigit():
                    recherche = (
                        search.split(" ")[1]
                        + " "
                        + search.split(" ")[0]
                        + " "
                        + search.split(" ")[2]
                    )
                else:
                    recherche = (
                        search.split(" ")[1]
                        + " "
                        + search.split(" ")[2]
                        + " "
                        + search.split(" ")[0]
                    )
            else:
                recherche = search.split(" ")[0]
          
            recherche = recherche.replace("AP Southeast", "(Asia Pacific) Southeast Asia").replace("AP East", "(Asia Pacific) East Asia")
            recherche = recherche.lower().replace(" ", "")

            # print("Recherche 2",recherche)
            texte_recherches+=recherche + " "

        
            for loc in locations:
                if (
                    loc["id"].lower().replace(" ", "") == recherche
                    or loc["name"].lower().replace(" ", "") == recherche
                    or recherche in loc["regionalDisplayName"].lower().replace(" ", "")
                ):
                    results.append(loc)
        
        
        
            # print("Result2")
            # print(results)
            # print()
            

        if len(results) == 0:
            splittedSearch = search.replace("JA ", "Japan ").split(" ")

            if splittedSearch[0]=="CH":
                # print(search.replace(" ","").lower())
                results.append({
                    'id': f'/subscriptions/{subscriptionId}/locations/'+search.replace(" ","").lower(),
                    'name': search.replace(" ","").lower(),
                    'type': 'Region',
                    'regionalDisplayName':search,
                    'latitude': None,
                    'longitude': None,
                    'physicalLocation': 'Switzerland',
                    'regionCategory': 'Recommended',
                    'country': "Suisse"
                    })

            elif splittedSearch[0]=="CA":
                 results.append({
                    'id': f'/subscriptions/{subscriptionId}/locations/'+search.replace(" ","").lower(),
                    'name': search.replace(" ","").lower(),
                    'type': 'Region',
                    'regionalDisplayName':search,
                    'latitude': None,
                    'longitude': None,
                    'physicalLocation': 'Canada',
                    'regionCategory': 'Recommended',
                    'country': "Canada"
                    })
            elif splittedSearch[0]=="AU":
                 results.append({
                    'id': f'/subscriptions/{subscriptionId}/locations/'+search.replace(" ","").lower(),
                    'name': search.replace(" ","").lower(),
                    'type': 'Region',
                    'regionalDisplayName':search,
                    'latitude': None,
                    'longitude': None,
                    'physicalLocation': 'Australia',
                    'regionCategory': 'Recommended',
                    'country': "Australie"
                    })
            elif splittedSearch[0]=="BR":
                 results.append({
                    'id': f'/subscriptions/{subscriptionId}/locations/'+search.replace(" ","").lower(),
                    'name': search.replace(" ","").lower(),
                    'type': 'Region',
                    'regionalDisplayName':search,
                    'latitude': None,
                    'longitude': None,
                    'physicalLocation': 'Brazil',
                    'regionCategory': 'Recommended',
                    'country': "Brésil"
                    })
                 
                    



        if len(results) == 0:
            splittedSearch = search.replace("JA ", "Japan ").split(" ")
            recherche = (
                pycountry.countries.search_fuzzy(splittedSearch[0])[0].name + " " + splittedSearch[1]
            )

            recherche = recherche.lower().replace(" ", "")
            # print("Recherche 3",search)
            texte_recherches+=recherche + " "


            for loc in locations:
                if (
                    loc["id"].lower().replace(" ", "") == recherche
                    or loc["name"].lower().replace(" ", "") == recherche
                    or recherche in loc["regionalDisplayName"].lower().replace(" ", "")
                ):
                    results.append(loc)
                    
                    
       

        if len(results)==0:


            recherche = (
                splittedSearch[1]+" "+pycountry.countries.search_fuzzy(splittedSearch[0])[0].name
            )
            recherche = recherche.lower().replace(" ", "")
            texte_recherches+=recherche + " "


            for loc in locations:
                if (
                    loc["id"].lower().replace(" ", "") == recherche
                    or loc["name"].lower().replace(" ", "") == recherche
                    or recherche in loc["regionalDisplayName"].lower().replace(" ", "")
                ):
                    results.append(loc)
                
            # print("Result3")
            # print(results)
            # print()
            
        if len(results) > 1:
            for loc2 in results:
                if loc2["name"] == recherche or loc2["regionalDisplayName"] == recherche:
                    results2.append(loc2)
            results=results2

        if len(results) > 1:
            # print("Filtre sur recommended : "+search)
            for loc3 in results:
                if loc3["regionCategory"] == "Recommended":
                    results3.append(loc3)
            results = results3

        if len(results3) == 1:
            results = results3
        if len(results2) == 1:
            results = results2

        if len(results) == 1:
            # print("Unique résultat : " + search + " => " + results[0]["name"])
            return results[0]
        elif len(results) == 0:
            # print("Aucune région trouvée : ", subscriptionId,"=>",search,"=>",texte_recherches)
            return {'id': None,
                'name': search,
                'type': "Interzone",
                'regionalDisplayName': search,
                'latitude': None,
                'longitude': None,
                'physicalLocation': search,
                'regionCategory': 'Interzone',
                'country': 'Interzone'}
            # pass
        else:
            print(
                "Plusieurs régions trouvées : "
                + search
                + " => "
                + ", ".join([res["name"] for res in results])
            )
            # print()
    return results

def get_infos_resource(service, resourceId, resourceType):
    if len(resourceType.replace("microsoft.", "").split("/")) > 1:
        rType = (
            resourceType.lower().replace("microsoft.", "").split("/")[1].capitalize()
        )
    else:
        rType = resourceType.lower().replace("microsoft.", "").split("/")[0].capitalize()

    infos = {
        "ResourceId": resourceId,
        "ResourceName": resourceId.split("/")[-1].capitalize(),
        "ResourceType": rType,
        "ServiceName": service,
        "ServiceType": resourceType.lower().replace("microsoft.", "").split("/")[0].capitalize(),  # TODO Compute, Storage, Network, Security, Backup...
        "ServiceInfraType": None,  # TODO IaaS, SaaS, PaaS
    }
    return infos


# 
# get_infos_location(subscriptions,"56603fd2-c2c5-4189-b513-e09ff0c4cc05","Canada Central")


# subscriptions=get_subscriptions()
# %store subscriptions



# %%
def get_application(subscriptions,subscriptionId,resourceGroup):    
    
    infosSubscription=get_infos_subscription(subscriptions,subscriptionId)
    
    if len(infosSubscription)>0:
      infosSubscription=infosSubscription[0]
      # print(subscriptionId,":",infosSubscription)
      
      abonnement = infosSubscription["subscriptionName"]
      if "RETAIL" in abonnement :
        application="Advisor"
      
      elif "CONSUMER" in abonnement:
        application="Watch ID"
      
      elif "INNOV" in abonnement:
        application="Innovation"
      
      elif "IOT" in abonnement:
        application="IOT"
      
      elif "VDI" in abonnement or "COLLAB" in  abonnement:
        application="Collaboratif"

      elif "IT-PROD-DATAHUB" in abonnement or "IT-PROD-XC" in  abonnement:
        application="Hubs"
          
      elif "BYOK" in abonnement:
        application="Sécurité"
        
      elif "EVENT" in abonnement:
        application="Evénements"
        
      elif "WSR"  in  abonnement:
        application="WSR"
        
      else: 
        if "CMSS" in resourceGroup.upper() or "APIG" in resourceGroup.upper() or "SERVICEBUS" in resourceGroup.upper():
          application="Core Model (WSA)"
        
        elif "BI" in resourceGroup.upper():
          application="Core Model (BI)"
          
        elif "WSR" in resourceGroup.upper():
          application="Core Model (WSR)"
          
        elif "INFRA" in resourceGroup.upper():
          application="Core Model (Infra)"
        elif "D365FO" in resourceGroup.upper():
          application="Core Model (D365)"
        else:
          # print("Core Model",subscriptionId,":",resourceGroup)
          application="Core Model"
          
      environnement = "Autres"
          
      if "prod" in resourceGroup.lower() or "log" in resourceGroup.lower() or "secu" in resourceGroup.lower():
        environnement="Prod"
        
      if "test" in resourceGroup.lower() or "dev" in resourceGroup.lower() or "qual" in resourceGroup.lower() or "demo" in resourceGroup.lower() or "poc" in resourceGroup.lower() or  "preprod" in resourceGroup.lower() or "int" in resourceGroup.lower():
        environnement="Non Prod"

      
      return {
        "Abonnement":abonnement,
        "Application":application,
        "Environnement":environnement
      }
      
    # print("Get Application => ",subscriptionId,":",resourceGroup)
    return {
        "Abonnement":None,
        "Application":None,
        "Environnement":None
      }

# %%
def getTokenValidity(access_token):
    expires_in = access_token.get('expires_in')
    # Calculer la date d'expiration
    if expires_in is not None:
        expires_in = int(expires_in)
        expiration_date = datetime.now() + timedelta(seconds=expires_in)
    else:
        expiration_date = None
    
    return expiration_date

def getToken():    
    clientId="258d0525-a80c-4b29-bf60-9bd65ac5d418"
    clientSecret="p3Q8Q~VOkEdF-hC-wfuNf.aUrwycYNiNpAibAakZ"
    payload = {
        "grant_type":"client_credentials",
        "client_id":clientId,
        "scope":"https://management.core.windows.net/.default openid profile offline_access",
        "client_secret":clientSecret
        }
    headers = {'Content-type': 'application/x-www-form-urlencoded', 'Accept': 'application/json'}
    
    reponse = REQ.post( "https://login.microsoftonline.com/f2460eca-756e-4a3f-bd14-d2a84590fc31/oauth2/v2.0/token", data=payload,headers=headers)
    return reponse.json()['access_token']

token=getToken()

# %%
resourcesDevTest[(resourcesDevTest["ServiceName"]=="Azure App Service")&(resourcesDevTest["ProjectName"]=="IT-DEV")].groupby("Date")[["CostOnDemand","CostReservation1Y"]].sum()

# %%
resourcesDevTest[(resourcesDevTest["ServiceName"]=="Azure App Service")&(resourcesDevTest["ProjectName"]=="IT-DEV")].groupby("Date")[["CostOnDemand","CostReservation1Y","CostDevTest"]].sum()

# %%
def doRequest(mode,url,payload):
    global token
    
    try:
        if token==None:
            HEADERS=   {'Content-Type':'application/json'}
        else:
            HEADERS ={"Content-Type":"application/json","Authorization": "Bearer {}".format(token)}
        
        #print("requête 1 : "+url)
        if mode=="GET" :
            response= REQ.get(url,headers=HEADERS)

        elif mode=="POST":
            response=REQ.post(url,headers=HEADERS, json=payload)

        if response == 400:
            return 400

        if not response is None  and response != 400 and response.status_code == 429:
            if 'x-ms-ratelimit-microsoft.costmanagement-qpu-remaining' in response.headers.keys() and int(response.headers['x-ms-ratelimit-microsoft.costmanagement-qpu-remaining'].split(",")[0].replace("QueriesPerHour:",""))==0 :
                print(f"Limite de requêtes atteinte, veuillez réessayer dans ({int(int(response.headers['x-ms-ratelimit-microsoft.costmanagement-qpu-retry-after'])/60)} mins)")
                return None
            if 'x-ms-ratelimit-microsoft.costmanagement-qpu-retry-after' in response.headers.keys():
                delaiQpu=response.headers['x-ms-ratelimit-microsoft.costmanagement-qpu-retry-after']
                #print(response.headers)
            else:
                delaiQpu=0
                
            if 'x-ms-ratelimit-microsoft.costmanagement-entity-retry-after' in response.headers.keys():
                delaiEntity=response.headers['x-ms-ratelimit-microsoft.costmanagement-entity-retry-after']
            else:
                delaiEntity=0
                
                
            if "x-ms-ratelimit-microsoft.costmanagement-clienttype-retry-after" in response.headers.keys():
                delaiClient =response.headers['x-ms-ratelimit-microsoft.costmanagement-clienttype-retry-after']
            else:
                delaiClient=0
                
            delaiTot=(int(delaiEntity)+int(delaiQpu)+int(delaiClient))*1.5
            
            if delaiTot==0:
                delaiTot=60
            print(f"[DoRequest] Délai 429 : {delaiEntity} + {delaiQpu} + {delaiClient} = {delaiTot} secs")
            time.sleep(max(delaiTot,1))
            
            #print("requête 2 : "+url)
            response=doRequest(mode,url,payload)
            
            if not response is None  and response != 400 and response.status_code == 429 :
                time.sleep(82)
                print(f"[DoRequest] Délai 429 : 80 secs")
                response=doRequest(mode,url,payload)
                
        elif  not response is None  and response != 400 and (response.status_code == 401 or response.status_code == 403):
            token=getToken()
            #print("requête 4 : "+url)
            response=doRequest(mode,url,payload)
        
        elif not response is None  and response != 400 and response.status_code == 504:
            time.sleep(35)
            print(f"[DoRequest] Délai 504 : 80 secs")
            response=doRequest(mode,url,payload)

        elif not response is None  and response != 400 and response.status_code == 400 and "does not have any valid subscriptions" in response.text:
            return 400

        if response == 400:
            return 400
        
        if not response is None and response.status_code == 200:
            return response
        else:
            if response==None:
                print("[DoRequest] Error")
            else:
                json_string = json.loads(response.text)
                if "error" in json_string.keys() and "code" in json_string["error"].keys():
                    error = json_string["error"]["code"]
                    message=json_string["error"]["message"]
                    print(f"Error {response.status_code}: {error} {message} | {url}")
                else:
                    print(f"Error {response.text}")
            return None

    except ConnectionError as e:
        print(f"Erreur de connexion : {e}")
        # print(f"Tentative de requête {retry + 1}/{max_retries} après {retry_delay} secondes...")
        if "10054" in str(e):
            time.sleep(80)
            token=getToken()
            return doRequest(mode,url,payload)
    

        
def doQueryCostManagement(scope, body):
    url = "https://management.azure.com/"+scope+"/providers/Microsoft.CostManagement/query?api-version=2021-10-01"
    print(url)
    response = doRequest("POST",url, body)
    return response

# %%
# mgGroup="mg-affiliates"
# subscriptions=get_subscriptions()
token = getToken()

# %%
# body ={
#   "type": "AmortizedCost",
#   "timeframe":{
#             "from": "2022-01-01T00:00:000Z",
#             "to": "2023-12-31T23:59:000Z"
#         },
#         "Dataset": {
#     "granularity": "Monthly",
#             "aggregation": {
#                 "totalCost": {
#                     "name": "Cost",
#                     "function": "Sum"
#                 },
#                 "UsageQuantity": {
#                     "name": "UsageQuantity",
#                     "function": "Sum",
#                     "unit": "Hours"
#                 }
#             },
#     "filter": {
#         {
#           "dimensions": {
            # "name": "Currency",
#             "operator": "In",
#             "values": [
#               "CHF"
#             ]
#           }
#         }
#     }
    
#   }
  
body = {
        "type": "AmortizedCost",
        "timeframe": "Custom",
        "timePeriod": {
            "from": "2024-01-01T00:00",
          "to": "2024-01-31T23:59"
        },
        "Dataset": {
            "granularity": "Monthly",
            "aggregation": {
                "totalCost": {
                    "name": "Cost",
                    "function": "Sum"
                },
                "UsageQuantity": {
                    "name": "UsageQuantity",
                    "function": "Sum",
                    "unit": "Hours"
                }
            },
            "grouping": [
                {
                    "type": "Dimension",
                    "name": "ServiceName"
                }
            #     # {
            #     #     "type": "Dimension",
            #     #     "name": "MeterId"
            #     # },
            #     # {
            #     #     "type": "Dimension",
            #     #     "name": "SubscriptionId"
            #     # },
            #     # {
            #     #     "type": "Dimension",
            #     #     "name": "PricingModel"
            #     # },
            #     # {
            #     #     "type": "Dimension",
            #     #     "name": "ReservationName"
            #     # },
            #     #  {
            #     #     "type":"Dimension",
            #     #     "name":"ChargeType"
            #     # },
            #     #  {
            #     #     "type":"Dimension",
            #     #     "name":"ServiceTier"
            #     # },
            #     #  {
            #     #     "type":"Dimension",
            #     #     "name":"ResourceLocation"
            #     # },
            #     #  {
            #     #     "type": "Dimension",
            #     #     "name": "ResourceId"
            #     # },
            #     #  {
            #     #     "type": "Dimension",
            #     #     "name": "ResourceType"
            #     # },
            #     #  {
            #     #     "type": "Dimension",
            #     #     "name": "Meter"
            #     # },
            #     #  {
            #     #     "type": "Dimension",
            #     #     "name": "MeterSubCategory"
            #     # },
                 
            #     #  {
            #     #     "type": "Dimension",
            #     #     "name": "ResourceGuid"
            #     # }
            ]
        
        
    # "filter": {
    #   "dimensions": {
    #     "name": "SubscriptionId",
    #     "operator": "In",
    #     "values": ['669420be-9749-47c9-be7e-8b7c6f042a25',
    #    'fd3c055d-e2a9-4280-b262-19e504763038',
    #    '56603fd2-c2c5-4189-b513-e09ff0c4cc05',
    #    'c2fcec5b-0ca1-45ea-bf18-d4373e75ccb2',
    #    '83ead8b4-d021-42d5-a744-bbfd7571c062',
    #    'b7b9c92a-9511-4758-9450-1f66e0d8688e',
    #    '31df7fd5-6704-4a90-8a7f-9a2bf849c500',
    #    '972140b0-c27a-4db2-9a2e-c276118c4dbb',
    #    'e2a728fc-d37c-4187-b154-62a35757c889',
    #    '3c22f584-b385-4111-9083-4ab84cbc80dd',
    #    'c1d6f862-c192-44ed-944f-c473de873c50',
    #    '61496e03-237c-426e-963d-c90b5eee31ca',
    #    '993253e1-7a07-4d24-93ee-b8e3c82591e4',
    #    'ecd89902-fea2-43f2-8b0c-6f33b2813e97',
    #    '68db32be-c003-4400-91e1-2cd7e3bc1323',
    #    '8d5552b1-0631-4da9-ac32-931980a54230',
    #    'c704e026-58a8-4170-a937-b5f2b1bd61c0',
    #    '0dd7b83f-bd0b-4bf2-b1e3-c504468f35a7',
    #    'e7411ba3-b844-4e45-baa5-f84fc35ef0d0',
    #    '37687f29-0ead-4d57-aefc-91d8e2cbe452',
    #    '14731824-aafc-4d86-b844-3d90f8eacd65',
    #    'b9bc9446-b623-40a4-86c3-6ba56b943bf1',
    #    '4861fca8-1d79-4566-a45d-54f4f6e2e9b5',
    #    '97ecae77-6d20-46ac-9da6-b19fd371bd8e',
    #    'aeb00cb7-ba08-4182-a70e-f0272630d3b1',
    #    'd7e168b1-f642-4d7f-92e9-9d4df218a807']
    #   }
    # }
    }
  
}

subscriptionId="669420be-9749-47c9-be7e-8b7c6f042a25"
scope=f"subscriptions/{subscriptionId}"
response=doQueryCostManagement(scope, body)
response.json()["properties"]["rows"]



# doQueryCostManagement("mg-rolex",body)

# %% [markdown]
# Facturations Mois

# %%
def get_lignes_facturation_mois(scope,d,skipToken=""):
    
    startTime=time.time()
     # Définition des paramètres de l'API Cost Management
    url = "https://management.azure.com/"+scope+"/providers/Microsoft.CostManagement/query?api-version=2021-10-01"
    if not skipToken=="":
        url+=("&$skiptoken="+skipToken)
        
    #api_version = "2019-11-01-preview"

    # Récupération du token d'accès Azure
    start_date,end_date = get_current_month(d,True)
    # end_date = datetime.now()
    # start_date = end_date - timedelta(days=365)
    # Récupération des coûts du mois en cours pour le Management Group spécifié
    #url = "{}/{}".format(base_url, management_group_id)
    body = {
        "type": "AmortizedCost",
        "timeframe": "Custom",
        "timePeriod": {
            "from": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        },
        "Dataset": {
            "granularity": "Monthly",
            "aggregation": {
                "totalCost": {
                    "name": "Cost",
                    "function": "Sum"
                },
                "UsageQuantity": {
                    "name": "UsageQuantity",
                    "function": "Sum",
                    "unit": "Hours"
                }
            },
            "grouping": [
                {
                    "type": "Dimension",
                    "name": "ResourceGroupName"
                },
                {
                    "type": "Dimension",
                    "name": "ServiceName"
                },
                {
                    "type": "Dimension",
                    "name": "MeterId"
                },
                {
                    "type": "Dimension",
                    "name": "SubscriptionId"
                },
                {
                    "type": "Dimension",
                    "name": "PricingModel"
                },
                {
                    "type": "Dimension",
                    "name": "ReservationName"
                },
                #  {
                #     "type":"Dimension",
                #     "name":"ChargeType"
                # },
                 {
                    "type":"Dimension",
                    "name":"ServiceTier"
                },
                 {
                    "type":"Dimension",
                    "name":"ResourceLocation"
                },
                 {
                    "type": "Dimension",
                    "name": "ResourceId"
                },
                 {
                    "type": "Dimension",
                    "name": "ResourceType"
                },
                 {
                    "type": "Dimension",
                    "name": "Meter"
                },
                 {
                    "type": "Dimension",
                    "name": "MeterSubCategory"
                },
                 
                 {
                    "type": "Dimension",
                    "name": "ResourceGuid"
                },
                {
                    "type": "Dimension",
                    "name": "PartNumber"
                }
            ]
        
        }
    }
    
    costs = []
    counter = 1
    
    while len(url)>0:
        
        response = doRequest("POST",url, body)

        skipToken=""
        
        if response is None and not response == 400:
            token=getToken()
            #print("Sleep",response)
            time.sleep(80)
            response = doRequest("POST",url, body)
        
        
        if not response is None and not response == 400:
            
            data = response.json()
            print(d,str(counter)+" : "+url)  
            
            for item in data["properties"]["rows"]:


                # cost = {
                #     "Date": item[2].replace("T00:00:00",""),
                #     # "JoursMois":max(nombre_jours_mois(item[2].replace("T00:00:00","")),1),
                #     "Subscription": item[6],
                #     "ResourceGroup": item[3],
                #     "Service": item[4],
                #     "PricingModel":item[7],
                #     "Reservation":item[8],
                #     "Usage": item[1],
                #     "Coût": item[0] if item[17]=="CHF" else item[0]*0.9,
                #     "ChargeType":item[9],
                #     "ServiceTier":item[10],
                #     "Location":item[11],
                #     "ResourceId":item[12],
                #     "ResourceType":item[13],
                #     "MeterId":item[5],
                #     "MeterName":item[14],
                #     "MeterCategory":item[15],
                #     "ResourceGuid":item[16],
                #     "PartNumber":item[17],
                #     "Currency":item[18]                
                # }

                cost = {
                    "Date": item[2].replace("T00:00:00",""),
                    # "JoursMois":max(nombre_jours_mois(item[2].replace("T00:00:00","")),1),
                    "Subscription": item[6],
                    "ResourceGroup": item[3],
                    "Service": item[4],
                    "PricingModel":item[7],
                    "Reservation":item[8],
                    "Usage": item[1],
                    "Coût": item[0] if item[17]=="CHF" else item[0]*0.9,
                    # "ChargeType":item[9],
                    "ServiceTier":item[9],
                    "Location":item[10],
                    "ResourceId":item[11],
                    "ResourceType":item[12],
                    "MeterId":item[5],
                    "MeterName":item[13],
                    "MeterCategory":item[14],
                    "ResourceGuid":item[15],
                    "PartNumber":item[16],
                    "Currency":item[17]                
                }
                costs.append(cost)
            
            url=data["properties"]["nextLink"]
            print(data["properties"]["nextLink"])
            if url=="" or url==None:
                url=""
                skipToken=""
            else:
                # print(url)
                skipToken=url.split("$skiptoken=")[1]
            counter+=1
            
        else:
            if response==400:
                print(scope,"Pas d'abonnement pour la période",start_date.strftime("%Y-%m-%d"),)
            else:
                print(scope,"Requête incomplète",start_date.strftime("%Y-%m-%d")) 
            return (costs,skipToken)
    
    print(scope,"Requête complète", start_date.strftime("%Y-%m-%d"), str(int((time.time()-startTime)/60)) + " minutes")
    return (costs,skipToken) 

def get_lignes_facturations(scope,start_date=None,end_date=None,lignesFacturationAzure=None):
    listeMois=generate_month_list(start_date,end_date)
    
    if lignesFacturationAzure==None:
        lignesFacturation=[]    
    else:
        lignesFacturation=lignesFacturationAzure
        
    for mois in listeMois:
        costs,skipToken = get_lignes_facturation_mois(scope,mois,"")


        if skipToken=="":
            lignesFacturation+=costs
            print(scope,"Traitement complet ("+mois.strftime("%Y-%m-%d")+")")
        else:
            print(scope,"Traitement incomplet ("+mois.strftime("%Y-%m-%d")+") : ",skipToken)
       # break
    return lignesFacturation





# %%
# subscriptionId="669420be-9749-47c9-be7e-8b7c6f042a25"
# scope=f"subscriptions/{subscriptionId}"

scope=f"providers/Microsoft.Management/managementGroups/mg-affiliates"
lignes_Facturation_DevTest = get_lignes_facturations(scope,start_date="2022-01-01",end_date="2024-01-01",lignesFacturationAzure=None)

# %% [markdown]
# Transformation Ressources

# %%
def get_ressources(subscriptions,lignesFacturation):

    lignesFacturation.sort(key=lambda x:(x["ResourceGuid"],x["Date"]))
    
    
    resources =[]
    for indiceLigne in range(len(lignesFacturation)):
        ligne=lignesFacturation[indiceLigne]
        # if ligne["ResourceId"]=="/subscriptions/e7411ba3-b844-4e45-baa5-f84fc35ef0d0/resourcegroups/rg-gb-infra-network/providers/microsoft.compute/virtualmachines/nf-azgb-r8040" and ligne["Date"]=="2022-07-01":
        newResource=True
        indice=None
        resource=None
        for i in range(len(resources)) : 
            res = resources[len(resources)-i-1]
            if res["Date"]==ligne["Date"] and res["ProjectId"]==ligne["Subscription"] and res["ResourcePath"]==ligne["ResourceId"] and res["ServiceName"]==ligne["Service"] and res["ServiceTier"]==ligne["ServiceTier"] and res["ResourceId"]==ligne["ResourceGuid"] and res["ResourceSkuSize"]==ligne["MeterName"] and res["ResourceSkuTier"]==ligne["MeterCategory"]:
                newResource=False
                indice=len(resources)-i-1
                resource=res
                break
            
            if i>100:
                break
        
        if newResource and resource==None:
            application=get_application(subscriptions,ligne["Subscription"],ligne["ResourceGroup"])
            # print("Application",application,indiceLigne)
            location=get_infos_location(subscriptions,ligne["Subscription"],ligne["Location"])
            # print("Location",location)

            service=get_infos_resource(ligne["Service"],ligne["ResourceId"],ligne["ResourceType"])
            # print("Service",service)

            resource = {
                "Date":ligne["Date"],
                
                "Provider":"Azure",
                    
                "ProjectId":ligne["Subscription"], #Azure
                "ProjectName":application["Abonnement"],
                "ResourceGroupName":ligne["ResourceGroup"],

                "ApplicationName":application["Application"],
                "Environment":application["Environnement"],#TODO

                "LocationId":None,                
                "LocationName":None,
                "LocationCountry":None,
                "LocationRegion":None,

                "ServiceId":None, #TODO
                "ServiceName":ligne["Service"],
                "ServiceType":service["ServiceType"], 
                "ServiceInfraType":service["ServiceInfraType"], #TODO
                "ServiceTier":ligne["ServiceTier"],
                
                "ResourceId":ligne["ResourceGuid"],
                "ResourcePath":ligne["ResourceId"],
                "ResourceName":service["ResourceName"].lower(),#TODO
                "ResourceType":service["ResourceType"],                

                "ResourceSkuId":None, #TODO
                "ResourceSkuTier":ligne["MeterCategory"],
                "ResourceSkuSize":ligne["MeterName"],
                "PartNumber":[],

                "ResourceTags":[], #TODO
                "ResourceConfiguration":{}, #TODO

                "PricingModels": [], #TODO
                "PricingUnit":None, #TODO
                "PricingDiscount":None,   #TODO

                "UsageTot":0, 
                "UsageOnDemand":0, #TODO
                "UsageReservation1Y":0, #TODO
                "UsageReservation3Y":0, #TODO
                "UsageDevTest":0, #TODO
                "UsageSpot":0, #TODO

                "CostTot":0, 
                "CostOnDemand":0, #TODO
                "CostReservation1Y":0, #TODO
                "CostReservation3Y":0, #TODO
                "CostDevTest":0, #TODO
                "CostSpot":0, #TODO
                
                "Currency":ligne["Currency"]
            
            }

        if location!=None:
            # resource["LocationId"]=location["id"]
            # resource["LocationName"]=location["displayName"]
            # resource["LocationRegion"]=location["regionalDisplayName"]
            # resource["LocationCountry"]=location["country"]
            resource["LocationName"]=location["name"]
            resource["LocationLatitude"]=location["latitude"]
            resource["LocationLongitude"]=location["longitude"]
            resource["LocationId"]=location["id"]
            resource["LocationCountry"]=location["country"]
            resource["LocationCategory"]=location["regionCategory"]

        resource["UsageTot"]+=ligne["Usage"]
        resource["CostTot"]+=ligne["Coût"] #TODO à condition que ce soit la même devise

        if ligne["PricingModel"]=="Reservation":
            if not ligne["Reservation"]==None and ("_1y".lower() in ligne["Reservation"].lower() or "_y".lower() in ligne["Reservation"].lower()) :
                model="Reservation1Y"
            elif not ligne["Reservation"]==None and "_3y".lower() in ligne["Reservation"].lower():
                model="Reservation3Y"
            else:
                model=None
                print("Erreur",ligne["Reservation"])
                
        elif ligne["PricingModel"]=="Spot":
            model="Spot"
        else:
            if ligne["PricingModel"]!="OnDemand":
                print(ligne["PricingModel"])
            model="OnDemand"
        
        if not model in resource["PricingModels"]:
            resource["PricingModels"].append(model)
        
        if ligne["PartNumber"] in liste_partnumbers_dev_test:
            model="DevTest"

        resource["PartNumber"].append({
            "partNumber":ligne["PartNumber"],
            "cost":ligne["Coût"],
            "usage":ligne["Usage"],
            "unitCost":format_unit_price(ligne["Coût"]/max(ligne["Usage"],0.001),ligne["PartNumber"],ligne["Date"],sku=ligne["MeterCategory"]+" "+ligne["MeterName"],nominal = True),
            "pricingModel":model,
            "sku":ligne["MeterCategory"]+" "+ligne["MeterName"]
        })

        resource["Usage"+model]+=ligne["Usage"]
        resource["Cost"+model]+=ligne["Coût"] #TODO à condition que ce soit la même devise

        # print(indiceLigne," ",newResource,ligne["Date"],"-",resource["ResourceSkuTier"],":",ligne["MeterCategory"],"|",resource["ResourceSkuSize"],":",ligne["MeterName"]," => ",len(resources))
        # print(resource)
        # print()
        # if not ligne["MeterCategory"] in resource["ResourceSkuTiers"]:
        #     resource["ResourceSkuTiers"].append(ligne["MeterCategory"])
            
        # if not ligne["MeterName"] in resource["ResourceSkuSizes"]:
        #     resource["ResourceSkuSizes"].append(ligne["MeterName"])
        
        if newResource:
            resources.append (resource)   
        else:
            resources[indice]=resource
    
        if indiceLigne%1000==0:
            print("Avancement","{:.2%}".format(indiceLigne/len(lignesFacturation))," %", len(resources))
            
            
        #return resources
    return pd.DataFrame(resources)


# %%
def format_unit_price(unitPrice,partNumber, date,sku=None,mesure=None,nominal=False):
    jours = get_count_days(date)
    
    if nominal:
        # print(unitPrice,partNumber,date)
        if "Connection" in sku:
            return unitPrice/(jours*24)

    else:
        if not mesure == None:
            
            hours=float(mesure.split(" ")[0])
            
        elif "Month" in mesure:
            if nominal:
                hours=jours*24
            else:
                hours=float(mesure.split(" ")[0])*jours*24
        # print(hours)
        unitPrice = unitPrice/hours    
    return unitPrice


# %%
resourcesDevTest=get_ressources(subscriptions,lignes_Facturation_DevTest)
%store lignes_Facturation_DevTest resourcesDevTest

# %% [markdown]
# Instances SQL non utilisées

# %%
def agg_func(x):
    unique_items = []
    for item_list in list(x):
        if type(item_list)==list:
            for item in item_list:            
                if not item in unique_items:
                    unique_items.append(item)
                    
        else:
            if not item_list in unique_items:
                unique_items.append(item_list)
                
    return unique_items


def check_naming_convention(name):
    if name.count('-') == 4 and '_' not in name:
        return 'Compliant'
    
    elif name.count('-') > 4 and '_' not in name:
        return 'To check'
    
    elif name=="ssisdb":
        return "Compliant"
    
    else:
        return 'Non compliant'

# %%
def get_SQL_Maximums(resourcePath,index,startTime=datetime(2023,1,1,00,00,00), endTime=datetime(2023,10,1,0,0,0)):
    
    # print (index,resourcePath)
    
    # Connection successful
    api_url = f"https://management.azure.com{resourcePath}/providers/microsoft.insights/metrics?api-version=2019-07-01&metricnames=connection_successful&aggregation=count&interval=P1D&timespan="+startTime.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+endTime.strftime("%Y-%m-%dT%H:%M:%SZ")
    response = doRequest("GET",api_url,"")

    if not response==None and response.status_code == 200 :
        data = response.json()
        
        if "value" in data.keys() and len(data["value"])>0 and "timeseries" in data["value"][0] and len(data["value"][0]["timeseries"])>0  and "data" in data["value"][0]["timeseries"][0]:
            compteur=pd.DataFrame.from_dict(data["value"][0]["timeseries"][0]["data"]) #Connection successful
            # print(compteur.head())
            
            compteur["Date"]=compteur["timeStamp"].apply(lambda row:row.split("T")[0])
            compteur["Heure"]=compteur["timeStamp"].apply(lambda row:row.split("T")[1])
            compteur=compteur.fillna(0.0)
            compteur=compteur.rename(columns={"count":"MaxSuccessfulConnection"})
            
            
            if not "MaxSuccessfulConnection" in compteur.head():
                compteur["MaxSuccessfulConnection"]=compteur.apply(lambda r : 0.0)
            
            MaxSucessfulConnection=max(compteur["MaxSuccessfulConnection"])

        else:
            MaxSucessfulConnection=0.0
        
    else:
        MaxSucessfulConnection=None
        print("Erreur lors de l'appel à l'API:", response)

    if not MaxSucessfulConnection==None:
        #CPU percentage
        api_url = f"https://management.azure.com{resourcePath}/providers/microsoft.insights/metrics?api-version=2019-07-01&metricnames=cpu_percent&aggregation=maximum&interval=P1D&timespan="+startTime.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+endTime.strftime("%Y-%m-%dT%H:%M:%SZ")
        response = doRequest("GET",api_url,"")

        if not response==None and response.status_code == 200 :
            data = response.json()
            
            if "value" in data.keys() and len(data["value"])>0 and "timeseries" in data["value"][0] and len(data["value"][0]["timeseries"])>0  and "data" in data["value"][0]["timeseries"][0]:
                compteur=pd.DataFrame.from_dict(data["value"][0]["timeseries"][0]["data"]) #CPU Percentage
                # print(compteur.head())
                
                compteur["Date"]=compteur["timeStamp"].apply(lambda row:row.split("T")[0])
                compteur["Heure"]=compteur["timeStamp"].apply(lambda row:row.split("T")[1])
                compteur=compteur.fillna(0.0)
                compteur=compteur.rename(columns={"maximum":"MaxCPUPercentage"})
                
                
                if not "MaxCPUPercentage" in compteur.head():
                    compteur["MaxCPUPercentage"]=compteur.apply(lambda r : 0.0)
                
                MaxCPUPercentage=max(compteur["MaxCPUPercentage"])
            else:
                MaxCPUPercentage=0.0
        else:
            MaxCPUPercentage=0.0
            print("Erreur lors de l'appel à l'API:", response)


        #Session count
        api_url = f"https://management.azure.com{resourcePath}/providers/microsoft.insights/metrics?api-version=2019-07-01&metricnames=sessions_count&aggregation=maximum&interval=P1D&timespan="+startTime.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+endTime.strftime("%Y-%m-%dT%H:%M:%SZ")
        response = doRequest("GET",api_url,"")

        if not response==None and response.status_code == 200 :
            data = response.json()
            
            if "value" in data.keys() and len(data["value"])>0 and "timeseries" in data["value"][0] and len(data["value"][0]["timeseries"])>0  and "data" in data["value"][0]["timeseries"][0]:
                
                compteur=pd.DataFrame.from_dict(data["value"][0]["timeseries"][0]["data"]) #Session count
                # print(compteur.head())
                
                compteur["Date"]=compteur["timeStamp"].apply(lambda row:row.split("T")[0])
                compteur["Heure"]=compteur["timeStamp"].apply(lambda row:row.split("T")[1])
                compteur=compteur.fillna(0.0)
                compteur=compteur.rename(columns={"maximum":"MaxSessionCount"})
                
                
                if not "MaxSessionCount" in compteur.head():
                    compteur["MaxSessionCount"]=compteur.apply(lambda r : 0.0)
                
                MaxSessionCount=max(compteur["MaxSessionCount"])  
                
            else:
                MaxSessionCount=0.0  
            
            # return compteur[["Date","Heure","MaxSuccessfulConnection"]]
        else:
            MaxSessionCount=0.0
            print("Erreur lors de l'appel à l'API:", response)
        
        return MaxSessionCount,MaxSucessfulConnection,MaxCPUPercentage
    return None

def get_unused_sql_database(mois1="2021-01-01",mois2="2021-01-01"):
    
    instances = resourcesAzure[resourcesAzure["ServiceName"]=="SQL Database"]
    dates=sorted(list(instances["Date"].unique()))
    final_date=dates[-1]
    
    list_subscriptions=instances[instances["Date"]==final_date]["ProjectId"].unique()
    
    start_date1,end_date1=get_current_month(generate_month_list(mois1,mois2)[0],False)
    start_date2,end_date2=get_current_month(generate_month_list(mois1,mois2)[1],False)
    # subscriptionId=list_subscriptions[0]
    
    
    sqlDatabase=pd.DataFrame(columns=["Date","ProjectId","ProjectName","ResourceGroupName","LocationName","ResourceName","ResourceSkuSize","CostTot","PricingModels","Perfs "+mois1,"Perfs "+mois2])
    counter=0
    
    # list_subscriptions=["669420be-9749-47c9-be7e-8b7c6f042a25"]
    for subscriptionId in list_subscriptions:
        url=f"https://management.azure.com/subscriptions/{subscriptionId}/providers/Microsoft.Sql/servers?api-version=2023-02-01-preview"
        
        while not url == None and len(url)>0:
            print(counter,url)
            response=doRequest("GET",url,"")

            if not response == None and response.status_code==200:
                data=response.json()
                                
                if len(data["value"])>0:
                    rowSQLs=data["value"]
                    # return rowSQLs
                    
                    df_sql=pd.DataFrame(rowSQLs)
                    df_sql["Date"]=final_date
                    df_sql["ProjectId"]=df_sql["id"].apply(lambda val:val.split("/")[2])
                    df_sql["ResourceGroupName"]=df_sql["id"].apply(lambda val:val.split("/")[4])
                    df_sql["ServiceName"]="SQL Database"
                    df_sql["ResourceName"]=df_sql["name"]
                    df_sql["ServerName"]=df_sql["properties"].apply(lambda row:row["fullyQualifiedDomainName"])
                    df_sql["LocationName"]=df_sql["location"]
                    # df_apps["Reserved"]=df_apps["properties"].apply(lambda row:row["reserved"])
                    # df_apps["LastModifiedTime"]=df_apps["properties"].apply(lambda row:row["lastModifiedTimeUtc"])

                    # return df_sql
                    
                    dbs= df_sql.merge(instances,on=["Date","ProjectId","ResourceName"],how="inner",suffixes=["x",""])[["Date","ProjectId","ProjectName","ResourceGroupName","LocationName","ServiceName","ServiceTier","ResourceName","ResourcePath","ResourceSkuSize","CostTot","PricingModels"]]
                    # dbs=dbs[dbs["ServiceName"]=="SQL Database"]
                    
                    # return sites
                    sqlDatabase=pd.concat([sqlDatabase,dbs])
                    # return sqlDatabase

                    # return azure_apps
                   # df_apps["ResourceSku"]=df_apps["sku"].apply(lambda row:row["properties"][1]["Reserved"])
                    # df_disks["ResourceSkuSize"]=df_disks["sku"].apply(lambda row:row["name"])
                    # df_disks["TimeCreated"]=df_disks["properties"].apply(lambda row:row["timeCreated"])
                    # df_disks["DiskState"]=df_disks["properties"].apply(lambda row:row["diskState"])

                    # disks=pd.concat([disks,df_disks[df_disks["DiskState"]=="Unattached"].merge(resourcesAzure,on=["Date","ProjectId","ResourceName","LocationName"],how="inner",suffixes=["x",""])[["Date","ProjectId","ProjectName","ResourceGroupName","LocationName","ResourceName","ResourceSkuSize","CostTot","PricingModels","CostTot","PricingModels"]]])


                    if "nextLink" in data.keys():
                        url=data["nextLink"]

                    else:
                        url=""
                    
                    
                else:
                    url=""
            else:
                url=""
                
            counter+=1
                        # return MaxSessionCount,MaxSucessfulConnection,MaxCPUPercentage

    # return sqlDatabase
    sqlDatabase = sqlDatabase.groupby('ResourceName').agg({
        'Date': 'first',  # Prenez la première date
        'ProjectId': 'first',  # Prenez la première ProjectId
        'ProjectName': 'first',  # Prenez la première ProjectName
        'ResourceGroupName': 'first',  # Prenez la première ResourceGroupName
        'LocationName': 'first',  # Prenez la première LocationName
        'ServiceName': 'first',  # Prenez la première ServiceName
        'ServiceTier': agg_func,  # Prenez la première ServiceTier
        'ResourceSkuSize': agg_func,  # Prenez la première ResourceSkuSize
        'ResourcePath': 'first',  # Prenez la première ResourcePath
        'PricingModels': agg_func,  # Prenez la première PricingModels
        'CostTot': 'sum'  # Sommez les coûts
    }).reset_index()
    
    sqlDatabase["CheckNamingConvention"]=sqlDatabase["ResourceName"].apply(lambda row:check_naming_convention(row))
    # return sqlDatabase
    
    if sqlDatabase.empty:
        return sqlDatabase
    
    sqlDatabase["Perfs "+mois1]=sqlDatabase.apply(lambda row:get_SQL_Maximums(row["ResourcePath"],row.name,start_date1,end_date1),axis=1,result_type='expand')
    sqlDatabase["Perfs "+mois2]=sqlDatabase.apply(lambda row:get_SQL_Maximums(row["ResourcePath"],row.name,start_date2,end_date2),axis=1,result_type='expand')

    
    if "Perfs "+mois1 in sqlDatabase.columns:
        sqlDatabase["MaxSessionCount "+mois1] = sqlDatabase["Perfs "+mois1].apply(lambda row: row[0] if (isinstance(row, tuple) and len(row) > 0) else 0)
        sqlDatabase["MaxSucessfulConnection "+mois1]=sqlDatabase["Perfs "+mois1].apply(lambda row:row[1] if (isinstance(row, tuple) and len(row) > 0) else 0)
        sqlDatabase["MaxCPUPercentage "+mois1]=sqlDatabase["Perfs "+mois1].apply(lambda row:row[2] if (isinstance(row, tuple) and len(row) > 0) else 0)

    if "Perfs "+mois2 in sqlDatabase.columns:
        sqlDatabase["MaxSessionCount "+mois2]=sqlDatabase["Perfs "+mois2].apply(lambda row:row[0] if (isinstance(row, tuple) and len(row) > 0) else 0)
        sqlDatabase["MaxSucessfulConnection "+mois2]=sqlDatabase["Perfs "+mois2].apply(lambda row:row[1] if (isinstance(row, tuple) and len(row) > 0) else 0)
        sqlDatabase["MaxCPUPercentage "+mois2]=sqlDatabase["Perfs "+mois2].apply(lambda row:row[2] if (isinstance(row, tuple) and len(row) > 0) else 0)

    sqlDatabase.to_excel(f"C:\\Users\\hugo.dufrene\\Wavestone\\WE - ROLEX [CH] - Support mise en place Gouvernance Cloud public - 13665 - Documents partages\\Gouvernance Cloud\\FinOps\\Optimisations\\Décomissionnement\\SQL Database {mois1} {mois2}.xlsx",encoding="utf-8",index=False)
    return sqlDatabase

unused_sql_database=get_unused_sql_database(mois1="2023-09-01",mois2="2023-10-01")
%store unused_sql_database


# %% [markdown]
# Instances App Service non utilisées

# %%
def get_App_Service_Maximums(resourcePath,index,startTime=datetime(2023,1,1,00,00,00), endTime=datetime(2023,9,1,0,0,0)):
    
    # print (index,resourcePath)
    
    # CPU TIME MAX
    api_url = f"https://management.azure.com{resourcePath}/providers/microsoft.insights/metrics?api-version=2019-07-01&metricnames=CpuTime&aggregation=maximum&interval=P1D&timespan="+startTime.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+endTime.strftime("%Y-%m-%dT%H:%M:%SZ")
    response = doRequest("GET",api_url,"")
    # return response

    if not response==None and not response.status_code==404:
        if not response==None and response.status_code == 200 :
            data = response.json()
            # return data
            if "value" in data.keys() and len(data["value"])>0 and "timeseries" in data["value"][0] and len(data["value"][0]["timeseries"])>0  and "data" in data["value"][0]["timeseries"][0]:
                compteur=pd.DataFrame.from_dict(data["value"][0]["timeseries"][0]["data"]) #CPU TIME
                # print(compteur.head())
                
                compteur["Date"]=compteur["timeStamp"].apply(lambda row:row.split("T")[0])
                compteur["Heure"]=compteur["timeStamp"].apply(lambda row:row.split("T")[1])
                compteur=compteur.fillna(0.0)
                compteur=compteur.rename(columns={"maximum":"MaxCPUTime"})
                
                
                if not "MaxCPUTime" in compteur.head():
                    compteur["MaxCPUTime"]=compteur.apply(lambda r : 0.0)
                
                MaxCPUTime=max(compteur["MaxCPUTime"])

            else:
                MaxCPUTime=0.0
            
        else:
            MaxCPUTime=None
            print("Erreur lors de l'appel à l'API:", response)
        # return MaxCPUTime

        # if not MaxCPUTime==None:
        #Max Requests
        api_url = f"https://management.azure.com{resourcePath}/providers/microsoft.insights/metrics?api-version=2019-07-01&metricnames=requests&aggregation=maximum&interval=P1D&timespan="+startTime.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+endTime.strftime("%Y-%m-%dT%H:%M:%SZ")
        response = doRequest("GET",api_url,"")

        if not response==None and response.status_code == 200 :
            data = response.json()
            
            if "value" in data.keys() and len(data["value"])>0 and "timeseries" in data["value"][0] and len(data["value"][0]["timeseries"])>0  and "data" in data["value"][0]["timeseries"][0]:
                compteur=pd.DataFrame.from_dict(data["value"][0]["timeseries"][0]["data"]) #Max Requests
                # print(compteur.head())
                
                compteur["Date"]=compteur["timeStamp"].apply(lambda row:row.split("T")[0])
                compteur["Heure"]=compteur["timeStamp"].apply(lambda row:row.split("T")[1])
                compteur=compteur.fillna(0.0)
                compteur=compteur.rename(columns={"maximum":"MaxRequests"})
                
                
                if not "MaxRequests" in compteur.head():
                    compteur["MaxRequests"]=compteur.apply(lambda r : 0.0)
                
                MaxRequests=max(compteur["MaxRequests"])
            else:
                MaxRequests=0.0
        else:
            MaxRequests=0.0
            print("Erreur lors de l'appel à l'API:", response)
        # return MaxRequests

        #MaxMemoryWorkingSet
        api_url = f"https://management.azure.com{resourcePath}/providers/microsoft.insights/metrics?api-version=2019-07-01&metricnames=MemoryWorkingSet&aggregation=maximum&interval=P1D&timespan="+startTime.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+endTime.strftime("%Y-%m-%dT%H:%M:%SZ")
        response = doRequest("GET",api_url,"")

        if not response==None and response.status_code == 200 :
            data = response.json()
            
            if "value" in data.keys() and len(data["value"])>0 and "timeseries" in data["value"][0] and len(data["value"][0]["timeseries"])>0  and "data" in data["value"][0]["timeseries"][0]:
                
                compteur=pd.DataFrame.from_dict(data["value"][0]["timeseries"][0]["data"]) #Session count
                # print(compteur.head())
                
                compteur["Date"]=compteur["timeStamp"].apply(lambda row:row.split("T")[0])
                compteur["Heure"]=compteur["timeStamp"].apply(lambda row:row.split("T")[1])
                compteur=compteur.fillna(0.0)
                compteur=compteur.rename(columns={"maximum":"MaxMemoryWorkingSet"})
                
                
                if not "MaxMemoryWorkingSet" in compteur.head():
                    compteur["MaxMemoryWorkingSet"]=compteur.apply(lambda r : 0.0)
                
                MaxMemoryWorkingSet=max(compteur["MaxMemoryWorkingSet"])  
                
            else:
                MaxMemoryWorkingSet=0.0  
            
            # return compteur[["Date","Heure","MaxSuccessfulConnection"]]
        else:
            MaxMemoryWorkingSet=0.0
            print("Erreur lors de l'appel à l'API:", response)

        #FunctionExecutionCount
        api_url = f"https://management.azure.com{resourcePath}/providers/microsoft.insights/metrics?api-version=2019-07-01&metricnames=FunctionExecutionCount&aggregation=maximum&interval=P1D&timespan="+startTime.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+endTime.strftime("%Y-%m-%dT%H:%M:%SZ")
        response = doRequest("GET",api_url,"")

        if not response==None and response.status_code == 200 :
            data = response.json()
            
            if "value" in data.keys() and len(data["value"])>0 and "timeseries" in data["value"][0] and len(data["value"][0]["timeseries"])>0  and "data" in data["value"][0]["timeseries"][0]:
                
                compteur=pd.DataFrame.from_dict(data["value"][0]["timeseries"][0]["data"]) #Session count
                # print(compteur.head())
                
                compteur["Date"]=compteur["timeStamp"].apply(lambda row:row.split("T")[0])
                compteur["Heure"]=compteur["timeStamp"].apply(lambda row:row.split("T")[1])
                compteur=compteur.fillna(0.0)
                compteur=compteur.rename(columns={"maximum":"FunctionExecutionCount"})
                
                
                if not "FunctionExecutionCount" in compteur.head():
                    compteur["FunctionExecutionCount"]=compteur.apply(lambda r : 0.0)
                
                FunctionExecutionCount=max(compteur["FunctionExecutionCount"])  
                
            else:
                FunctionExecutionCount=0.0  
            
            # return compteur[["Date","Heure","MaxSuccessfulConnection"]]
        else:
            FunctionExecutionCount=0.0
            print("Erreur lors de l'appel à l'API:", response)
        # return MaxMemoryWorkingSet
        return MaxCPUTime,MaxRequests,MaxMemoryWorkingSet,FunctionExecutionCount
    return None

def get_unused_azure_app_service(donnees, mois1="2021-01-01",mois2="2021-01-01"):
    dates=sorted(list(donnees.copy()["Date"].unique()))
    final_date=dates[-1]
    
    list_subscriptions=donnees[donnees["Date"]==final_date]["ProjectId"].unique()
    
    start_date1,end_date1=get_current_month(generate_month_list(mois1,mois2)[0],False)
    start_date2,end_date2=get_current_month(generate_month_list(mois1,mois2)[1],False)
    
    
    azure_apps=pd.DataFrame(columns=["Date","ProjectId","ProjectName","ResourceGroupName","LocationName","ResourceName","ResourceSkuSize","CostTot","PricingModels","Perfs "+mois1,"Perfs "+mois2])
    counter=0
    
    # list_subscriptions=["669420be-9749-47c9-be7e-8b7c6f042a25"]
    for subscriptionId in list_subscriptions:
        url=f"https://management.azure.com/subscriptions/{subscriptionId}/providers/Microsoft.Web/sites?api-version=2022-03-01"
        
        while not url == None and len(url)>0:
            print(counter,url)
            response=doRequest("GET",url,"")

            if not response == None and response.status_code==200:
                data=response.json()
                                
                if len(data["value"])>0:
                    rowApps=data["value"]
                    
                    
                    df_apps=pd.DataFrame(rowApps)
                    df_apps["Date"]=final_date
                    df_apps["ProjectId"]=df_apps["id"].apply(lambda val:val.split("/")[2])
                    df_apps["ResourceGroupName"]=df_apps["id"].apply(lambda val:val.split("/")[4])
                    df_apps["ResourceName"]=df_apps["name"]
                    df_apps["LocationName"]=df_apps["location"]
                    df_apps["Reserved"]=df_apps["properties"].apply(lambda row:row["reserved"])
                    df_apps["LastModifiedTime"]=df_apps["properties"].apply(lambda row:row["lastModifiedTimeUtc"])

                    # return df_apps
                    
                    sites= df_apps.merge(donnees.copy(),on=["Date","ProjectId","ResourceName"],how="inner",suffixes=["x",""])[["Date","ProjectId","ProjectName","ResourceGroupName","LocationName","ServiceName","ServiceTier","ResourceName","ResourcePath","ResourceSkuSize","CostTot","PricingModels"]]

                    sites=sites[sites["ServiceName"]=="Azure App Service"]
                    
                    # return sites
                    azure_apps=pd.concat([azure_apps,sites])

                    # return azure_apps
                   # df_apps["ResourceSku"]=df_apps["sku"].apply(lambda row:row["properties"][1]["Reserved"])
                    # df_disks["ResourceSkuSize"]=df_disks["sku"].apply(lambda row:row["name"])
                    # df_disks["TimeCreated"]=df_disks["properties"].apply(lambda row:row["timeCreated"])
                    # df_disks["DiskState"]=df_disks["properties"].apply(lambda row:row["diskState"])

                    # disks=pd.concat([disks,df_disks[df_disks["DiskState"]=="Unattached"].merge(resourcesAzure,on=["Date","ProjectId","ResourceName","LocationName"],how="inner",suffixes=["x",""])[["Date","ProjectId","ProjectName","ResourceGroupName","LocationName","ResourceName","ResourceSkuSize","CostTot","PricingModels","CostTot","PricingModels"]]])


                    if "nextLink" in data.keys():
                        url=data["nextLink"]

                    else:
                        url=""
                    
                    
                else:
                    url=""
            else:
                url=""
                
            counter+=1
            
            
    azure_apps = azure_apps.groupby('ResourceName').agg({
        'Date': 'first',  # Prenez la première date
        'ProjectId': 'first',  # Prenez la première ProjectId
        'ProjectName': 'first',  # Prenez la première ProjectName
        'ResourceGroupName': 'first',  # Prenez la première ResourceGroupName
        'LocationName': 'first',  # Prenez la première LocationName
        'ServiceName': 'first',  # Prenez la première ServiceName
        'ServiceTier': agg_func,  # Prenez la première ServiceTier
        'ResourceSkuSize': agg_func,  # Prenez la première ResourceSkuSize
        'ResourcePath': 'first',  # Prenez la première ResourcePath
        'PricingModels': agg_func,  # Prenez la première PricingModels
        'CostTot': 'sum'  # Sommez les coûts
    }).reset_index()
    
    azure_apps["CheckNamingConvention"]=azure_apps["ResourceName"].apply(lambda row:check_naming_convention(row))
   
                
    azure_apps["Perfs "+mois1]=azure_apps.apply(lambda row:get_App_Service_Maximums(row["ResourcePath"],row.name,start_date1,end_date1),axis=1)
    azure_apps["Perfs "+mois2]=azure_apps.apply(lambda row:get_App_Service_Maximums(row["ResourcePath"],row.name,start_date2,end_date2),axis=1)
    
    azure_apps["MaxCPUTime "+mois1] = azure_apps["Perfs "+mois1].apply(lambda row: row[0] if (isinstance(row, tuple) and len(row) > 0) else 0)
    azure_apps["MaxRequests "+mois1]=azure_apps["Perfs "+mois1].apply(lambda row:row[1] if (isinstance(row, tuple) and len(row) > 0) else 0)
    azure_apps["MaxMemoryWorkingSet "+mois1]=azure_apps["Perfs "+mois1].apply(lambda row:row[2] if (isinstance(row, tuple) and len(row) > 0) else 0)
    azure_apps["FunctionExecutionCount "+mois1]=azure_apps["Perfs "+mois1].apply(lambda row:row[3] if (isinstance(row, tuple) and len(row) > 0) else 0)

    azure_apps["MaxCPUTime "+mois2]=azure_apps["Perfs "+mois2].apply(lambda row:row[0] if (isinstance(row, tuple) and len(row) > 0) else 0)
    azure_apps["MaxRequests "+mois2]=azure_apps["Perfs "+mois2].apply(lambda row:row[1] if (isinstance(row, tuple) and len(row) > 0) else 0)
    azure_apps["MaxMemoryWorkingSet "+mois2]=azure_apps["Perfs "+mois2].apply(lambda row:row[2] if (isinstance(row, tuple) and len(row) > 0) else 0)
    azure_apps["FunctionExecutionCount "+mois2]=azure_apps["Perfs "+mois2].apply(lambda row:row[3] if (isinstance(row, tuple) and len(row) > 0) else 0)


    return azure_apps
mois1="2023-12-01"
mois2="2024-01-01"
unused_azure_app_service=get_unused_azure_app_service(resourcesDevTest.copy(),mois1=mois1,mois2=mois2)
%store unused_azure_app_service

unused_azure_app_service.to_excel(f"C:\\Users\\hugo.dufrene\\Wavestone\\WE - ROLEX [CH] - Support mise en place Gouvernance Cloud public - 13665 - Documents partages\\Gouvernance Cloud\\FinOps\\Optimisations\\App Service\\20240204 - Azure App Service {mois1} {mois2}.xlsx",index=False)



# %% [markdown]
# Instances Data Factory non utilisées

# %%
def get_Azure_Data_Factory_Maximums(resourcePath,index,startTime=datetime(2023,1,1,00,00,00), endTime=datetime(2023,9,1,0,0,0)):
    
    print (index,resourcePath)
    
    # SucceedTriggerRuns
    api_url = f"https://management.azure.com{resourcePath}/providers/microsoft.insights/metrics?api-version=2019-07-01&metricnames=TriggerSucceededRuns&aggregation=maximum&interval=P1D&timespan="+startTime.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+endTime.strftime("%Y-%m-%dT%H:%M:%SZ")
    response = doRequest("GET",api_url,"")
    # return response

    if not response==None and not response.status_code==404:
        if not response==None and response.status_code == 200 :
            data = response.json()
            # return data
            if "value" in data.keys() and len(data["value"])>0 and "timeseries" in data["value"][0] and len(data["value"][0]["timeseries"])>0  and "data" in data["value"][0]["timeseries"][0]:
                compteur=pd.DataFrame.from_dict(data["value"][0]["timeseries"][0]["data"]) #CPU TIME
                # print(compteur.head())
                
                compteur["Date"]=compteur["timeStamp"].apply(lambda row:row.split("T")[0])
                compteur["Heure"]=compteur["timeStamp"].apply(lambda row:row.split("T")[1])
                compteur=compteur.fillna(0.0)
                compteur=compteur.rename(columns={"maximum":"TriggerSucceededRuns"})
                
                
                if not "TriggerSucceededRuns" in compteur.head():
                    compteur["TriggerSucceededRuns"]=compteur.apply(lambda r : 0.0)
                
                TriggerSucceededRuns=max(compteur["TriggerSucceededRuns"])

            else:
                TriggerSucceededRuns=0.0
            
        else:
            TriggerSucceededRuns=None
            print("Erreur lors de l'appel à l'API:", response)
        # return MaxCPUTime

        # if not MaxCPUTime==None:
        #Max Requests
        api_url = f"https://management.azure.com{resourcePath}/providers/microsoft.insights/metrics?api-version=2019-07-01&metricnames=PipelineSucceededRuns&aggregation=maximum&interval=P1D&timespan="+startTime.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+endTime.strftime("%Y-%m-%dT%H:%M:%SZ")
        response = doRequest("GET",api_url,"")

        if not response==None and response.status_code == 200 :
            data = response.json()
            
            if "value" in data.keys() and len(data["value"])>0 and "timeseries" in data["value"][0] and len(data["value"][0]["timeseries"])>0  and "data" in data["value"][0]["timeseries"][0]:
                compteur=pd.DataFrame.from_dict(data["value"][0]["timeseries"][0]["data"]) #Max Requests
                # print(compteur.head())
                
                compteur["Date"]=compteur["timeStamp"].apply(lambda row:row.split("T")[0])
                compteur["Heure"]=compteur["timeStamp"].apply(lambda row:row.split("T")[1])
                compteur=compteur.fillna(0.0)
                compteur=compteur.rename(columns={"maximum":"PipelineSucceededRuns"})
                
                
                if not "PipelineSucceededRuns" in compteur.head():
                    compteur["PipelineSucceededRuns"]=compteur.apply(lambda r : 0.0)
                
                PipelineSucceededRuns=max(compteur["PipelineSucceededRuns"])
            else:
                PipelineSucceededRuns=0.0
        else:
            PipelineSucceededRuns=0.0
            print("Erreur lors de l'appel à l'API:", response)
        # return MaxRequests

        #MaxMemoryWorkingSet
        api_url = f"https://management.azure.com{resourcePath}/providers/microsoft.insights/metrics?api-version=2019-07-01&metricnames=ActivitySucceededRuns&aggregation=maximum&interval=P1D&timespan="+startTime.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+endTime.strftime("%Y-%m-%dT%H:%M:%SZ")
        response = doRequest("GET",api_url,"")

        if not response==None and response.status_code == 200 :
            data = response.json()
            
            if "value" in data.keys() and len(data["value"])>0 and "timeseries" in data["value"][0] and len(data["value"][0]["timeseries"])>0  and "data" in data["value"][0]["timeseries"][0]:
                
                compteur=pd.DataFrame.from_dict(data["value"][0]["timeseries"][0]["data"]) #Session count
                # print(compteur.head())
                
                compteur["Date"]=compteur["timeStamp"].apply(lambda row:row.split("T")[0])
                compteur["Heure"]=compteur["timeStamp"].apply(lambda row:row.split("T")[1])
                compteur=compteur.fillna(0.0)
                compteur=compteur.rename(columns={"maximum":"ActivitySucceededRuns"})
                
                
                if not "ActivitySucceededRuns" in compteur.head():
                    compteur["ActivitySucceededRuns"]=compteur.apply(lambda r : 0.0)
                
                ActivitySucceededRuns=max(compteur["ActivitySucceededRuns"])  
                
            else:
                ActivitySucceededRuns=0.0  
            
            # return compteur[["Date","Heure","MaxSuccessfulConnection"]]
        else:
            ActivitySucceededRuns=0.0
            print("Erreur lors de l'appel à l'API:", response)

        #FunctionExecutionCount
        api_url = f"https://management.azure.com{resourcePath}/providers/microsoft.insights/metrics?api-version=2019-07-01&metricnames=IntegrationRuntimeCpuPercentage&aggregation=maximum&interval=P1D&timespan="+startTime.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+endTime.strftime("%Y-%m-%dT%H:%M:%SZ")
        response = doRequest("GET",api_url,"")

        if not response==None and response.status_code == 200 :
            data = response.json()
            
            if "value" in data.keys() and len(data["value"])>0 and "timeseries" in data["value"][0] and len(data["value"][0]["timeseries"])>0  and "data" in data["value"][0]["timeseries"][0]:
                
                compteur=pd.DataFrame.from_dict(data["value"][0]["timeseries"][0]["data"]) #Session count
                # print(compteur.head())
                
                compteur["Date"]=compteur["timeStamp"].apply(lambda row:row.split("T")[0])
                compteur["Heure"]=compteur["timeStamp"].apply(lambda row:row.split("T")[1])
                compteur=compteur.fillna(0.0)
                compteur=compteur.rename(columns={"maximum":"IntegrationRuntimeCpuPercentage"})
                
                
                if not "IntegrationRuntimeCpuPercentage" in compteur.head():
                    compteur["IntegrationRuntimeCpuPercentage"]=compteur.apply(lambda r : 0.0)
                
                IntegrationRuntimeCpuPercentage=max(compteur["IntegrationRuntimeCpuPercentage"])  
                
            else:
                IntegrationRuntimeCpuPercentage=0.0  
            
            # return compteur[["Date","Heure","MaxSuccessfulConnection"]]
        else:
            IntegrationRuntimeCpuPercentage=0.0
           
            print("Erreur lors de l'appel à l'API:", response)
        # return MaxMemoryWorkingSet
        if math.isnan(IntegrationRuntimeCpuPercentage):
            IntegrationRuntimeCpuPercentage=0.0
                
        return TriggerSucceededRuns,PipelineSucceededRuns,ActivitySucceededRuns,IntegrationRuntimeCpuPercentage
    return None

  
def get_unused_data_factory(mois1="2021-01-01",mois2="2021-01-01"):
    dates=sorted(list(resourcesAzure.copy()["Date"].unique()))
    final_date=dates[-1]
    
    list_subscriptions=resourcesAzure[resourcesAzure["Date"]==final_date]["ProjectId"].unique()
    
    
    start_date1,end_date1=get_current_month(generate_month_list(mois1,mois2)[0],False)
    start_date2,end_date2=get_current_month(generate_month_list(mois1,mois2)[1],False)

    
    dataFactory=pd.DataFrame(columns=["Date","ProjectId","ProjectName","ResourceGroupName","LocationName","ResourceName","ResourceSkuSize","CostTot","PricingModels","Perfs "+mois1,"Perfs "+mois2])
    counter=0
    
    # list_subscriptions=["669420be-9749-47c9-be7e-8b7c6f042a25"]
    for subscriptionId in list_subscriptions:
        url=f"https://management.azure.com/subscriptions/{subscriptionId}/providers/Microsoft.DataFactory/factories?api-version=2018-06-01"
        
        while not url == None and len(url)>0:
            print(counter,url)
            response=doRequest("GET",url,"")

            if not response == None and response.status_code==200:
                data=response.json()
                                
                if len(data["value"])>0:
                    rowApps=data["value"]
                    
                    df_apps=pd.DataFrame(rowApps)
                    df_apps["Date"]=final_date
                    df_apps["ProjectId"]=df_apps["id"].apply(lambda val:val.split("/")[2])
                    df_apps["ResourceGroupName"]=df_apps["id"].apply(lambda val:val.split("/")[4])
                    df_apps["ResourceName"]=df_apps["name"]
                    df_apps["LocationName"]=df_apps["location"]
                    df_apps["CreatedTime"]=df_apps["properties"].apply(lambda row:row["createTime"])

                                        
                    # return df_apps
                    
                    data_factories= df_apps.merge(resourcesAzure.copy(),on=["Date","ProjectId","ResourceName"],how="inner",suffixes=["x",""])[["Date","ProjectId","ProjectName","ResourceGroupName","LocationName","ServiceName","ServiceTier","ResourceName","ResourcePath","ResourceSkuSize","CostTot","PricingModels"]]

                    data_factories=data_factories[data_factories["ServiceName"]=="Azure Data Factory v2"]
                    
                    # return sites
                    dataFactory=pd.concat([dataFactory,data_factories])

                    # return azure_apps
                   # df_apps["ResourceSku"]=df_apps["sku"].apply(lambda row:row["properties"][1]["Reserved"])
                    # df_disks["ResourceSkuSize"]=df_disks["sku"].apply(lambda row:row["name"])
                    # df_disks["TimeCreated"]=df_disks["properties"].apply(lambda row:row["timeCreated"])
                    # df_disks["DiskState"]=df_disks["properties"].apply(lambda row:row["diskState"])

                    # disks=pd.concat([disks,df_disks[df_disks["DiskState"]=="Unattached"].merge(resourcesAzure,on=["Date","ProjectId","ResourceName","LocationName"],how="inner",suffixes=["x",""])[["Date","ProjectId","ProjectName","ResourceGroupName","LocationName","ResourceName","ResourceSkuSize","CostTot","PricingModels","CostTot","PricingModels"]]])


                    if "nextLink" in data.keys():
                        url=data["nextLink"]

                    else:
                        url=""
                    
                    
                else:
                    url=""
            else:
                url=""
                
            counter+=1
            
    dataFactory = dataFactory.groupby('ResourceName').agg({
        'Date': 'first',  # Prenez la première date
        'ProjectId': 'first',  # Prenez la première ProjectId
        'ProjectName': 'first',  # Prenez la première ProjectName
        'ResourceGroupName': 'first',  # Prenez la première ResourceGroupName
        'LocationName': 'first',  # Prenez la première LocationName
        'ServiceName': 'first',  # Prenez la première ServiceName
        'ServiceTier': agg_func,  # Prenez la première ServiceTier
        'ResourceSkuSize': agg_func,  # Prenez la première ResourceSkuSize
        'ResourcePath': 'first',  # Prenez la première ResourcePath
        'PricingModels': agg_func,  # Prenez la première PricingModels
        'CostTot': 'sum'  # Sommez les coûts
    }).reset_index()
    
    dataFactory["CheckNamingConvention"]=dataFactory["ResourceName"].apply(lambda row:check_naming_convention(row))
   
    dataFactory["Perfs "+mois1]=dataFactory.apply(lambda row:get_Azure_Data_Factory_Maximums(row["ResourcePath"],row.name,start_date1,end_date1),axis=1)
    dataFactory["Perfs "+mois2]=dataFactory.apply(lambda row:get_Azure_Data_Factory_Maximums(row["ResourcePath"],row.name,start_date2,end_date2),axis=1)
    
    dataFactory["TriggerSucceededRuns "+mois1] = dataFactory["Perfs "+mois1].apply(lambda row: row[0] if (isinstance(row, tuple) and len(row) > 0) else 0)
    dataFactory["PipelineSucceededRuns "+mois1]=dataFactory["Perfs "+mois1].apply(lambda row:row[1] if (isinstance(row, tuple) and len(row) > 0) else 0)
    dataFactory["ActivitySucceededRuns "+mois1]=dataFactory["Perfs "+mois1].apply(lambda row:row[2] if (isinstance(row, tuple) and len(row) > 0) else 0)
    dataFactory["IntegrationRuntimeCpuPercentage "+mois1]=dataFactory["Perfs "+mois1].apply(lambda row:row[3] if (isinstance(row, tuple) and len(row) > 0) else 0)

    dataFactory["TriggerSucceededRuns "+mois2]=dataFactory["Perfs "+mois2].apply(lambda row:row[0] if (isinstance(row, tuple) and len(row) > 0) else 0)
    dataFactory["PipelineSucceededRuns "+mois2]=dataFactory["Perfs "+mois2].apply(lambda row:row[1] if (isinstance(row, tuple) and len(row) > 0) else 0)
    dataFactory["ActivitySucceededRuns "+mois2]=dataFactory["Perfs "+mois2].apply(lambda row:row[2] if (isinstance(row, tuple) and len(row) > 0) else 0)
    dataFactory["IntegrationRuntimeCpuPercentage "+mois2]=dataFactory["Perfs "+mois2].apply(lambda row:row[3] if (isinstance(row, tuple) and len(row) > 0) else 0)


    dataFactory.to_excel(f"C:\\Users\\hugo.dufrene\\Wavestone\\WE - ROLEX [CH] - Support mise en place Gouvernance Cloud public - 13665 - Documents partages\\Gouvernance Cloud\\FinOps\\Optimisations\\Décomissionnement\\Azure Data Factory {mois1} {mois2}.xlsx",encoding="utf-8",index=False)
    return dataFactory

unused_data_factory=get_unused_data_factory(mois1="2023-09-01",mois2="2023-10-01")
%store unused_data_factory


# %% [markdown]
# Gains

# %%
def moyenneCouts(data,metric="CostTot",identifiant=None,resourceId=None,dateMin=None,dateMax=None):
    data = data.groupby(["Date",identifiant])["CostTot"].sum().reset_index()
    data=data[data[identifiant]==resourceId]
    
    if dateMin==None:
        data=data.sort_values("Date",ascending=True)
        countable=True
    # if dateMax==None:
    #     countable=False
    #     data=data.sort_values("Date",ascending=True)
    sum = 0.0
    count = 0.0
    
    moyenne=0.0
    # print(data)

    for i in range(min(3,len(data))):
        index=len(data)-i-2
        print(data["Date"].iloc[index],index,len(data))
        
        if data[identifiant].iloc[index]!=resourceId :
            print("ERREUR",resourceId)
            
        if (dateMin==None or datetime.strptime(data["Date"].iloc[index], "%Y-%m-%d").date()>datetime.strptime(dateMin, "%Y-%m-%d").date()) and (dateMax==None or datetime.strptime(data["Date"].iloc[index], "%Y-%m-%d").date()<datetime.strptime(dateMax, "%Y-%m-%d").date() ) :
            sum+=data[metric].iloc[index]
            count+=1
    if count==0:
        return 0,0
    # if dateMin==None:
    #     return sum/3,count #sum/count
    # else:
    return sum/count,count

# moyenneCouts(resourcesCoreModel.copy(),metric="CostTot",identifiant="ResourceName",resourceId="sqldb-xd-cmss-dev20-cmss",dateMax="2023-11-01")
moyenneCouts(resourcesCoreModel.copy(),metric="CostTot",identifiant="ResourceName",resourceId="sqldb-us-cmss-prod01-cmss_del2023-06-19_heat-1042708",dateMax="2023-11-01")

# %%
def get_decommissionnement(donnees,date_min="2020-01-01",identifiant="ResourceName"):
    metric="CostTot"
    donnees=donnees[donnees["Date"]>=date_min]
    
    donnees= donnees.sort_values([identifiant,"Date"])

    dates=list(donnees["Date"].unique())
    dates.sort()
    
    lignesDecomissionnement=[]

    for i in range(len(donnees)-2):
        row0=donnees.iloc[i]
        row1=donnees.iloc[i+1]
        row2=donnees.iloc[i+2]
        
        indexDate0 = dates.index(row0["Date"])
        indexDate1 = dates.index(row1["Date"])
        indexDate2 = dates.index(row2["Date"])
        
        # print(row0[identifiant],row1[identifiant],row2[identifiant],indexDate1,indexDate2)
        # if indexDate0==indexDate1-1 and row1["Date"]!=dates[-1] and row0["ResourceId"]==row1["ResourceId"] and (indexDate1!=indexDate2-1  or row1["ResourceId"]!=row2["ResourceId"]):
        if  row0[identifiant]==row1[identifiant] and row1[identifiant]!=row2[identifiant] and row1["Date"]!=dates[-1]:
            decommissionnement = {
                "Date":row1["Date"],
                "ProjectName":row1["ProjectName"],
                "Application":row1["ApplicationName"],
                "Service":row1["ServiceName"]
            }
            decommissionnement[identifiant]=row1[identifiant]
            # print(row1["Date"],":",row1["ResourceId"],"=>",moyenneCouts(resourcesAzure,metric,row1["ResourceId"],dateMax=row1["Date"]),row1["ServiceName"])
            moyenne,number = moyenneCouts(donnees.copy(),metric,identifiant,row1[identifiant],dateMax=row1["Date"])
            # print(moyenne,number)
            decommissionnement[metric]=-moyenne
            decommissionnement["Count"]=-number
            # print(row0[identifiant],row1[identifiant],row2[identifiant],moyenne,number)
            
            lignesDecomissionnement.append(decommissionnement)
            # print("Instance décomissionnée",dates[indexDate1],row0["ResourceId"])
        # else:
            # print("ERREUR")
        
        if i%1000==0:
            # break
            print("Avancement :","{:.0%}".format((i+1)/(len(donnees)-2)))
    print("Calcul de décommissionnement terminé")
    return pd.DataFrame(lignesDecomissionnement)

date_min="2022-10-01"

gains_decommissionnement_CoreModel =get_decommissionnement(resourcesCoreModel.copy(),date_min)
%store gains_decommissionnement_CoreModel 

# %%
def get_new_resources(resourcesAzure,metric="CostTot"):
    resourcesAzure= resourcesAzure.sort_values(["ResourceId","Date"],ascending=False)
    dates=list(resourcesAzure["Date"].unique())
    
    lignesNewResources=[]

    for i in range(0,len(resourcesAzure)-2):
        row0=resourcesAzure.iloc[i]
        row1=resourcesAzure.iloc[i+1]
        row2=resourcesAzure.iloc[i+2]
        
        indexDate0 = dates.index(row0["Date"])
        indexDate1 = dates.index(row1["Date"])
        indexDate2 = dates.index(row2["Date"])
        
        if row0["ResourceId"]==row1["ResourceId"] and row1["ResourceId"]!=row2["ResourceId"] and row1["Date"]!=dates[-1]:
            newResource = {
                "Date":row1["Date"],
                "ProjectName":row1["ProjectName"],
                "Application":row1["ApplicationName"],
                "Service":row1["ServiceName"],
                "ResourceId":row1["ResourceId"],
                
            }

            moyenne,number = moyenneCouts(resourcesAzure,metric,row1["ResourceId"],dateMin=row1["Date"])
            
            newResource[metric]=moyenne
            newResource["Count"]=number
            lignesNewResources.append(newResource)
            #print("Instance décomissionnée",dates[indexDate1],row0["ResourceId"])
        
        
        if i%1000==0:
            print("Avancement :","{:.0%}".format((i)/(len(resourcesAzure)-1)))

    return pd.DataFrame(lignesNewResources)

# lignesNewResources

# %%
def plot_cost_history(df,dates,metric="CostTot", showServices=False,showCount=False,cumule=True,includeAllResources=True, limit=5):
    # Convertir la colonne "date" en format de date
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Trier le DataFrame par ordre croissant de date
    df = df.sort_values('Date')

    dates=list(dates)
    dates.sort()
    # Créer une liste de traces pour chaque service
    traces = []

    locale.setlocale(locale.LC_ALL, '')
    if showCount:
        showMetric="Count"
    else:
        showMetric=metric
        
    if cumule:
        showMetric="Sum"+showMetric
    
        
    # Regrouper les coûts par date et calculer la somme des coûts pour chaque date
    if showServices:
        df_grouped= df.groupby(['Date', 'ServiceName'])[metric].sum().reset_index()
        
        # if includeAllResources:
        #     df_grouped = df.groupby(['Date',"ServiceName"])[[metric,"Count"]].sum().reset_index()
            
        # else:
        #     df_grouped = df.groupby(['Date',"ServiceName"])[[metric,"Count"]].sum().reset_index()

    
        df_grouped["Sum"+metric] = df_grouped.groupby('ServiceName')[metric].cumsum()
        # df_grouped["SumCount"] = df_grouped.groupby('ServiceName')['Count'].cumsum()

        df_grouped2 = df.groupby('ServiceName')[metric].agg(lambda x: abs(x).sum()).reset_index()
        services = df_grouped2.nlargest(limit, metric)['ServiceName'].unique()
            
        for i in range(len(services)):
            service=services[i]
            data = df_grouped[df_grouped['ServiceName'] == service].copy()
            
            indicesDates=[]
            for d in range(1,len( dates)-1):               

                if not dates[d] in data["Date"].dt.strftime("%Y-%m-%d").unique() and not d in indicesDates:
                    indicesDates.append(d)

                    newElt={
                        "Date":dates[d],
                        "Service":service
                    }
                    if cumule:
                        
                        data_filtered=data[data["Date"]==dates[d-1]]
                        newElt[metric]=data_filtered[metric].sum()
                        # newElt["Count"]=data_filtered["Count"].sum()
                        newElt["Sum"+metric]=data_filtered["Sum"+metric].sum()
                        # newElt["SumCount"]=data_filtered["SumCount"].sum()
                    else:
                        newElt[metric]=0.0
                        # newElt["Count"]=0.0
                        newElt["Sum"+metric]=0.0
                        # newElt["SumCount"]=0.0
                    data=pd.concat([data,pd.DataFrame(newElt,index=[0])], ignore_index=True)
            
                    data['Date'] = pd.to_datetime(data['Date'])
            data['Date'] = pd.to_datetime(data['Date'])
            data.sort_values("Date") 
            
            bars = go.Bar(
                x=data['Date'],
                y=data[showMetric],
                # y=data[metric],
                # text=round(data['CostTot'], 1),
                hovertemplate='%{y:,.0f}<extra></extra>',  # Modèle de survol avec une seule décimale
                name=service,
                marker_color="#0089D6"
            )
            
            traces.append(bars)
        
    else:
        df_grouped = df.groupby('Date')[metric].sum().reset_index()


        
        bars = go.Bar(
            x=df_grouped['Date'],
            y=df_grouped[metric],
            # text=round(data['CostTot'], 1),
            hovertemplate='%{y:,.0f}<extra></extra>',  # Modèle de survol avec une seule décimale
            name="Coûts",
            marker_color="#0089D6"
        )
        traces.append(bars)
    
    # Définir le layout du graphique
    layout = go.Layout(
        barmode='stack',  # Barres empilées
        xaxis=dict(title='Date'),
        yaxis=dict(title=metric),
        title='Historique des coûts',
        legend=dict(x=1.05, y=1, bgcolor='rgba(255, 255, 255, 0.5)'),
        margin=dict(l=75, r=50, t=80, b=80),
        showlegend=True
    )
    
    # Créer la figure
    fig = go.Figure(data=traces, layout=layout)
    fig.update_xaxes(
        tickmode='array',
        tickvals=df_grouped['Date'],
        tickangle=0,  # Angle de rotation des ticks
        tickformat='%m.%y',  # Format des ticks de date (optionnel)
    )
    
    
    # Afficher le graphique
    fig.show()

# metric="UsageTot"
# donnees=pd.concat([lignesDecommissionnement.copy(),lignesNewResources.copy()])
# plot_cost_history(donnees,dates=resourcesAzure["Date"].unique(),metric=metric,showServices=True,showCount=False,cumule=True,limit=10)
# plot_cost_history(donnees,dates=resourcesAzure["Date"].unique(),metric=metric,showServices=True,showCount=False,cumule=False,limit=10)



# metric="CostTot"
# lignesDecommissionnement = get_decommissionnement(resourcesAzure,metric=metric)
# lignesNewResources = get_new_resources(resourcesAzure,metric=metric)
# donnees=pd.concat([lignesDecommissionnement.copy(),lignesNewResources.copy()])
# plot_cost_history(donnees,dates=resourcesAzure["Date"].unique(),metric=metric,showServices=True,showCount=False,cumule=True,limit=10)
plot_cost_history(resourcesAzure.copy(),dates=resourcesAzure["Date"].unique(),metric="CostTot",showServices=False,showCount=False,cumule=False,limit=10)


# %%
def generate_color_shades(base_color):
    # Convertir la couleur de base de notation hexadécimale à RGB
    r, g, b = tuple(int(base_color[i:i+2], 16) for i in (0, 2, 4))

    # Convertir les valeurs RGB en valeurs de point flottant entre 0 et 1
    r, g, b = r / 255.0, g / 255.0, b / 255.0

    # Convertir les valeurs RGB en valeurs HSL
    h, l, s = colorsys.rgb_to_hls(r, g, b)

    # Générer une teinte foncée en diminuant la luminosité (l) de 20%
    dark_hls = (h, max(l - 0.2, 0), s)
    # Convertir la teinte foncée de HSL en RGB
    dark_rgb = colorsys.hls_to_rgb(*dark_hls)
    # Convertir les valeurs RGB en notation hexadécimale
    dark_color = '#{:02x}{:02x}{:02x}'.format(int(dark_rgb[0] * 255), int(dark_rgb[1] * 255), int(dark_rgb[2] * 255))

    # Générer une teinte claire en augmentant la luminosité (l) de 20%
    light_hls = (h, min(l + 0.2, 1), s)
    # Convertir la teinte claire de HSL en RGB
    light_rgb = colorsys.hls_to_rgb(*light_hls)
    # Convertir les valeurs RGB en notation hexadécimale
    light_color = '#{:02x}{:02x}{:02x}'.format(int(light_rgb[0] * 255), int(light_rgb[1] * 255), int(light_rgb[2] * 255))

    return dark_color, light_color


# %%
def plot_cost_decom(donnees,lignesDecommissionnement,service=None,cumule=True,date_min="2015-01-01"):
    locale.setlocale(locale.LC_ALL, '')
    
    colors=["FF0000","003AFF","23FF00","FFB900","00F3FF","FF0093","00FFB9","8B00FF","FFFF00","FF0097","B6FF00"]
    donnees=donnees.rename(columns={"ServiceName":"Service"})
    donnees=donnees[donnees["Date"]>=date_min]
    donnees=donnees[donnees["Service"]==service]
    # Convertir la colonne "date" en format de date
    dates=donnees["Date"].unique()
    donnees['Date'] = pd.to_datetime(donnees['Date'])
    
    
    # Trier le DataFrame par ordre croissant de date
    donnees = donnees.sort_values('Date')

    dates=list(dates)
    dates.sort()
    # Créer une liste de traces pour chaque service
    traces = []

    df_grouped=donnees.groupby(['Date',"Service"])["CostTot"].sum().reset_index()
    df_grouped=df_grouped.sort_values("CostTot",ascending=False)
    # return df_grouped

    df_grouped_decom=lignesDecommissionnement.groupby(['Date',"Service"])["CostTot"].sum().reset_index()
    df_grouped_decom=df_grouped_decom.sort_values("CostTot",ascending=False)
    
    # return df_grouped_decom.sort_values()
    # if cumule:
    # #     showMetric="Sum"+showMetric
    # if servic==None:
    #     services = df_grouped["Service"].unique()

   
    max_y = 0
    # for index_service in range(len(services)):
        # service=services[index_service]
        # base_color = colors[index_service]
        # dark_color, light_color = generate_color_shades(base_color)
    
    data=df_grouped[df_grouped["Service"]==service]
    data_decom=df_grouped_decom[df_grouped_decom["Service"]==service]
    
    data["Date"]=pd.to_datetime(data["Date"])
    data_decom["Date"]=pd.to_datetime(data_decom["Date"])
    
    df_merged = pd.merge(data,data_decom, on=['Date',"Service"], how='outer',suffixes=["","Decom"])
    df_merged=df_merged.fillna(0)
    
    df_merged["CostTotDecom"]=abs(df_merged["CostTotDecom"])

    df_merged["CostTotDecomCumule"]=df_merged["Date"].apply(lambda date:df_merged[df_merged["Date"]<=date]["CostTotDecom"].sum())
        
    df_merged["CostTotSansDecom"]=df_merged["CostTot"]+df_merged["CostTotDecom"]
    
    df_merged["CostTotSansDecomCumule"]=df_merged["CostTot"]+df_merged["CostTotDecomCumule"]

    df_merged=df_merged.sort_values("Date")
    

    max_y = max(max_y,df_merged["CostTotSansDecomCumule"].max())


    show_data=pd.concat([pd.DataFrame.from_dict([{
            "Date": (df_merged["Date"].iloc[0]- timedelta(days=30)).replace(hour=0).replace(minute=0).replace(second=0).replace(microsecond=0),
            "Service":service,
            "CostTot":115000,
            "CostTotDecom":0,
            "CostTotSansDecom":0,
            "CostTotDecomCumule":0,
            "CostTotSansDecomCumule":100000               
        }]),df_merged.copy()], ignore_index=True)
    
    show_data=df_merged.copy()
    # return show_data[show_data["Date"]<sorted(show_data["Date"])[-1]]["Date"].unique()
    
    if cumule:
        trace = go.Scatter(
            x=show_data[show_data["Date"]<sorted(show_data["Date"])[-1]]["Date"],  # Concaténation des coordonnées x dans l'ordre croissant et décroissant
            y=show_data[show_data["Date"]<sorted(show_data["Date"])[-1]]["CostTotSansDecomCumule"],  # Ajout de zéros à la fin pour fermer l'aire sous la courbe
            # fill='tozeroy',  # Remplit l'aire sous la courbe jusqu'à l'axe y=0
            mode='lines',  # Mode "lines" pour obtenir une ligne continue
            # fillcolor='rgba(0, 176, 246, 0.2)',  # Couleur de remplissage de l'aire sous la courbe
            name=service+" projeté",
            hovertemplate="%{y:,.0f} CHF",
            # dash='dash',
            line={"color":"#8C99A0"}
        )
        traces.append(trace)
        
    
        trace = go.Scatter(
            x=show_data[show_data["Date"]>=sorted(show_data["Date"])[-2]]["Date"],  # Concaténation des coordonnées x dans l'ordre croissant et décroissant
            y=show_data[show_data["Date"]>=sorted(show_data["Date"])[-2]]["CostTotSansDecomCumule"],  # Ajout de zéros à la fin pour fermer l'aire sous la courbe
            # fill='tozeroy',  # Remplit l'aire sous la courbe jusqu'à l'axe y=0
            mode='lines',  # Mode "lines" pour obtenir une ligne continue
            # fillcolor='rgba(0, 176, 246, 0.2)',  # Couleur de remplissage de l'aire sous la courbe
            name=service+" projeté (disponible prochain mois)",
            hovertemplate="%{y:,.0f} CHF",
            # dash='dash',
            line={"color":"#8C99A0","dash":"dash"}
        )
        traces.append(trace)
        
        # else:
        #     trace = go.Scatter(
        #         x=show_data["Date"],  # Concaténation des coordonnées x dans l'ordre croissant et décroissant
        #         y=show_data["CostTotSansDecom"],  # Ajout de zéros à la fin pour fermer l'aire sous la courbe
        #         # fill='tozeroy',  # Remplit l'aire sous la courbe jusqu'à l'axe y=0
        #         mode='lines',  # Mode "lines" pour obtenir une ligne continue
        #         # fillcolor='rgba(0, 176, 246, 0.2)',  # Couleur de remplissage de l'aire sous la courbe
        #         name=service+" projeté",
        #         hovertemplate="%{y:,.0f} CHF",
                
        #         line={"color":"#8C99A0","dash":"dash"}
        #     )
        
        max_value1=math.ceil(max(show_data["CostTotSansDecomCumule"])/10000)*10000
        min_value1=math.floor(min(show_data["CostTot"])/10000)*10000-20000
        ecart_value1=(max_value1-min_value1)/10000
        
        max_value2=math.ceil(max(show_data["CostTotDecom"])/10000)*10000
        # print(max_value1,min_value1,ecart_value1)
        print(max_value2)

        
        #Coûts totaux
        trace = go.Scatter(
            x=show_data['Date'],  # Concaténation des coordonnées x dans l'ordre croissant et décroissant
            y=show_data["CostTot"],  # Ajout de zéros à la fin pour fermer l'aire sous la courbe
            # fill='tozeroy',  # Remplit l'aire sous la courbe jusqu'à l'axe y=0
            mode='lines',  # Mode "lines" pour obtenir une ligne continue
            # fillcolor='rgba(0, 176, 246, 0.2)',  # Couleur de remplissage de l'aire sous la courbe
            name=service,
            hovertemplate="%{y:,.0f} CHF",
            # text=service  # Add the series name as the text for each point
            line={"color":"#006039"}
        )

        traces.append(trace)


        bars = go.Bar(
            x=df_merged['Date'],
            y=df_merged["CostTotDecom"],
            # text=round(data['CostTot'], 1),
            hovertemplate='%{y:,.0f}<extra></extra>',  # Modèle de survol avec une seule décimale
             yaxis="y2",
            # width= 5,  # Adjust this value to control spacing between bar groups
            name="Gains",
            marker_color="#8C99A0"
        )
        traces.append(bars)
        
    
     # Définir le layout du graphique
    layout = go.Layout(
        barmode='stack',  # Barres empilées
        xaxis=dict(title='Date'),
        yaxis=dict(title="Coût"),
        title='Estimation des gains générés par le décomissionnement',
        legend=dict(x=1.05, y=1, bgcolor='rgba(255, 255, 255, 0.5)'),
        margin=dict(l=75, r=50, t=80, b=80),
        showlegend=True
    )
    
    # Créer la figure
    fig = go.Figure(data=traces, layout=layout)
    
    # fig.update_layout(xaxis_range=[min(df_merged["Date"]),max(df_merged["Date"])])
    fig.update_layout(
        xaxis_range=[min(df_merged["Date"]),max(df_merged["Date"])],
        yaxis=dict(title='Coûts réels / Coûts projetés',rangemode='nonnegative'),
        yaxis2=dict(title='Gains décommissionnement', overlaying='y', side='right',rangemode='nonnegative',showgrid=False),
        legend=dict(x=1.05, y=1, bgcolor='rgba(255, 255, 255, 0.5)'),
        yaxis_range=[min_value1, max_value1],
        yaxis2_range=[0,max_value2*ecart_value1/2],
        bargap=0.5
        
    )
    
    fig.update_xaxes(
        tickmode='array',
        tickvals=df_grouped["Date"],
        tickangle=270,  # Angle de rotation des ticks
        tickformat='%m.%y',  # Format des ticks de date (optionnel)
    )
    
    
    # Afficher le graphique
    fig.show()
    df_merged=df_merged.rename(columns={"Service":"ServiceName"})

    return df_merged


# plot_cost_decom(resourcesAzure.copy(),gains_decommissionnement.copy(),services= ["Azure App Service"])
# plot_cost_decom(resourcesAzure.copy(),gains_decommissionnement[gains_decommissionnement["ProjectName"]=="IT-DEV"].copy(),services=services)

# %%
def get_decommissionnement_IT_DEV():
    gains_decommissionnement_IT_DEV = gains_decommissionnement.copy()
    gains_decommissionnement_IT_DEV=gains_decommissionnement_IT_DEV[gains_decommissionnement_IT_DEV["ProjectName"]=="IT-DEV"]
    gains_decommissionnement_IT_DEV["ResourceName"] = gains_decommissionnement_IT_DEV["ResourceName"].str.lower()
    
    environments = [
        "build01",
        "demo01",
        "dev01",
        "dev03",
        "dev04",
        "dev20",
        "int02",
        "lc01",
        "lc02",
        "lczd01",
        "lczd02",
        "lczd03",
        "lczd04",
        "lec01",
        "qual05",
        "ref01",
        "ref02",
        "rel01",
        "test01",
        "test03",
        "test04"
    ]
    
    
    result=pd.DataFrame(columns=["Date","Environnement","ProjectName","Application","ResourceName","GainTot"])
    
    for env  in environments:
        filtered_df = gains_decommissionnement_IT_DEV[gains_decommissionnement_IT_DEV["ResourceName"].str.contains(env)].copy()
        # resultat = filtered_df.groupby('Date').agg({
        #     'ProjectName': lambda x: ', '.join(set(x)),
        #     'Application': lambda x: ', '.join(set(x)),
        #     'Service': lambda x: ', '.join(set(x)),
        #     'ResourceName': ', '.join,  # Concaténez les valeurs avec une virgule
        #     'CostTot': 'sum',  # Sommez les valeurs
        # }).reset_index()
        filtered_df["Environnement"]=env
        result=pd.concat([result,filtered_df],axis=0) 

    # Filter the DataFrame based on "ResourceName" values containing any value from environnements
    
    result["GainTot"]=-result["CostTot"]
    result = result.drop('CostTot', axis=1)
    return result

#[gains_decommissionnement_IT_DEV["ResourceName"].str.contains("acc03")]
    

decom_IT_DEV = get_decommissionnement_IT_DEV()
decom_IT_DEV.to_csv("C:\\Users\\hugo.dufrene\\Wavestone\\WE - ROLEX [CH] - Support mise en place Gouvernance Cloud public - 13665 - Documents partages\\Gouvernance Cloud\\FinOps\\Optimisations\\Décomissionnement\\20231018 - Gains Decommissionnement IT-DEV.csv",index=False,encoding="utf-8-sig")

# %%
def get_rightsizing_IT_DEV():
    gains_rightsizing_IT_DEV = gains_rightsizing_app_service.copy()
    gains_rightsizing_IT_DEV=gains_rightsizing_IT_DEV[gains_rightsizing_IT_DEV["ProjectName"]=="IT-DEV"]
    gains_rightsizing_IT_DEV["ResourceName"] = gains_rightsizing_IT_DEV["ResourceName"].str.lower()
    
    environments = [
        "build01",
        "dev03",
        "dev04",
        "dev20",
        "int02",
        "lc02",
        "lczd01",
        "lczd02",
        "lczd03",
        "lczd04",
        "lec01",
        "qual05",
        "test01",
        "test03",
        "test04"
    ]
    
    gains_rightsizing_IT_DEV["GainRightSizing"]=-gains_rightsizing_IT_DEV.apply(lambda row : gains_rightsizing_IT_DEV[(gains_rightsizing_IT_DEV["Date"]==row["Date"])&(gains_rightsizing_IT_DEV["GainRightSizing"]<0)&(gains_rightsizing_IT_DEV["ServiceName"]==row["ServiceName"])]["GainRightSizing"].sum(),axis=1)

    result=pd.DataFrame(columns=["Date","Environnement","ProjectName","Application","ResourceName","GainTot"])
    
    for env  in environments:
        filtered_df = gains_rightsizing_IT_DEV[gains_rightsizing_IT_DEV["ResourceName"].str.contains(env)].copy()
        # resultat = filtered_df.groupby('Date').agg({
        #     'ProjectName': lambda x: ', '.join(set(x)),
        #     'Application': lambda x: ', '.join(set(x)),
        #     'Service': lambda x: ', '.join(set(x)),
        #     'ResourceName': ', '.join,  # Concaténez les valeurs avec une virgule
        #     'CostTot': 'sum',  # Sommez les valeurs
        # }).reset_index()
        filtered_df["Environnement"]=env
        result=pd.concat([result,filtered_df],axis=0) 
        return filtered_df

    # Filter the DataFrame based on "ResourceName" values containing any value from environnements
    
    result["GainTot"]=-result["CostTot"]
    result = result.drop('CostTot', axis=1)
    return result

#[gains_decommissionnement_IT_DEV["ResourceName"].str.contains("acc03")]
    

get_rightsizing_IT_DEV()
# decom_IT_DEV.to_csv("C:\\Users\\hugo.dufrene\\Wavestone\\WE - ROLEX [CH] - Support mise en place Gouvernance Cloud public - 13665 - Documents partages\\Gouvernance Cloud\\FinOps\\Optimisations\\Décomissionnement WSA\\Gains Decommissionnement IT-DEV.csv",index=False,encoding="utf-8-sig")

# %%
def get_config(row):
    if row["ServiceName"]=="Azure App Service":
        if "Connection" in row["ResourceSkuTier"]:
            return "Connection"
        # elif "F1" in row["ResourceSkuTier"]:
        #     return "Functions"
        return "Compute"
    
    elif row["ServiceName"]=="SQL Database":
        
        if "Data" in row["ResourceSkuSize"]:
            return "Storage"
        if "License" in row["ResourceSkuTier"]:
            return "Licence"
        
        
        if "Compute" in row["ResourceSkuTier"]:
            return "Compute"
        
        if "DTU" in row["ResourceSkuSize"] or  "vCore" in row["ResourceSkuSize"]:
            return "Compute"
        
    return ""

# %%
def get_rightisizing_infos(resourceName,Skus1,Skus3):
    option=False
    if len(Skus1)==1 and len(Skus3)==1:
        changement = Skus1[0]+" => "+Skus3[0]
        option=False
        # print("1 elt",changement)
    elif len(Skus1)==1 and len(Skus3)>1:
        for Sku in Skus3:
            if "App" in Sku or not " " in Sku:
                option=False
            else : 
               option=True
            
        print(resourceName,Skus1, "=> ", sorted(Skus3))
    else:
        print(resourceName,sorted(Skus1), "=> ", sorted(Skus3))
        
    
    # if 'P2 v2 App' in row1["ResourceSkuSize"] and 'P2 v3 App' in row3["ResourceSkuSize"]:
    #     valeur = "P2 v2 => P2 v3"
    # elif 'P3 v2 App' in row1["ResourceSkuSize"] and 'P2 v3 App' in row3["ResourceSkuSize"]:
    #     valeur = "P3 v2 => P2 v3"
        
    # else:
    #     valeur="Autre"

# get_rightisizing_infos("asp-xd-cmss-lc02", ['P2 v3 App'],['B1 App', 'IP SSL Connection'])

# %%
def get_unit_cost_on_demand_leg(row,negocie=True):
    rabais = 0.12
    print(row[["Date","ProjectName","ServiceName"]])
    if row["ServiceName"] in ["Azure App Service","Virtual Machines","SQL Database"]:
    
        if row["UsageOnDemand"]>0: 
            if row["Date"]>="2023-09-01":#Après négociation microsoft
                if negocie:  #negocie
                    # print(row["Date"],row["CostOnDemand"],row["UsageOnDemand"])
                    return row["CostOnDemand"]/row["UsageOnDemand"]
                
                else: #non negocie
                    return row["CostOnDemand"]/row["UsageOnDemand"]/(1-rabais)
                
            else:
                return row["CostOnDemand"]/row["UsageOnDemand"]
                
        else:#rechercher précédent
            # dataRecherche=resourcesAzure.copy()
            dataRecherche=dataTest[(dataTest["Date"]<row["Date"])&(dataTest["UsageOnDemand"]>0)].copy()
            if not dataRecherche.empty:
                dataRecherche = dataRecherche.sort_values(by="Date", ascending=False)
                if negocie:
                    if dataRecherche.iloc[0]["Date"]>="2023-09-01" and dataRecherche.iloc[0]["UnitCostOnDemandNegocie"]==dataRecherche.iloc[0]["UnitCostOnDemand"]:
                        # print("TEST1")
                        return dataRecherche.iloc[0]["UnitCostOnDemand"]*(1-rabais)
                    
                    elif dataRecherche.iloc[0]["UnitCostOnDemandNegocie"]>0:    
                        # print("Test2")  
                        return dataRecherche.iloc[0]["UnitCostOnDemandNegocie"]
                elif not negocie and dataRecherche.iloc[0]["UnitCostOnDemand"]!=None and dataRecherche.iloc[0]["UnitCostOnDemand"]>0:
                    
                    return dataRecherche.iloc[0]["UnitCostOnDemand"]
                        
                        
                        
                if get_unit_cost_on_demand(dataRecherche.iloc[0],negocie)==get_unit_cost_on_demand(dataRecherche.iloc[0],not negocie) and row["Date"]>="2023-09-01" and negocie:
                    return get_unit_cost_on_demand(dataRecherche.iloc[0],not negocie)*(1-rabais)
                else:
                    return get_unit_cost_on_demand(dataRecherche.iloc[0],negocie)
                
            else :
                return 0.0
    return 0.0

# %%
def get_unit_cost_on_demand_v1(data_source,row, service,negocie=True):
    result=0.0
    
    if service in ["Azure App Service","Virtual Machines","SQL Database"]:
        # key = (row["Date"], row["ProjectName"], row["ServiceName"],row["ResourceName"],negocie)
        # print(key)
        # if key in cache_unit_cost:
        #     return cache_unit_cost[key]
        
        rabais = 0.11

        if row["UsageOnDemand"]>0 and row["CostOnDemand"]>10: 
            if row["Date"]>="2023-09-01":#Après négociation microsoft
                if negocie:  #negocie
                    result= row["CostOnDemand"]/row["UsageOnDemand"]
                
                else: #non negocie
                    result= row["CostOnDemand"]/row["UsageOnDemand"]/(1-rabais)
                
            else:
                result= row["CostOnDemand"]/row["UsageOnDemand"]
                # print("Test",row["Date"],row["CostOnDemand"],row["UsageOnDemand"],result)

        else:#rechercher précédent
            # dataRecherche=resourcesAzure.copy()
            print(row["Date"],"Recherche précédent")
            dataRecherche=data_source[(data_source["Date"]<row["Date"])&(data_source["UsageOnDemand"]>0)&(data_source["ServiceName"]==service)].copy()
            
            if not dataRecherche.empty:
                dataRecherche = dataRecherche.sort_values(by="Date", ascending=False)
                
                if negocie:
                    if dataRecherche.iloc[0]["Date"]>="2023-09-01" and dataRecherche.iloc[0]["UnitCostOnDemandNegocie"]==dataRecherche.iloc[0]["UnitCostOnDemand"]:
                        # print("TEST1")
                        result= dataRecherche.iloc[0]["UnitCostOnDemand"]*(1-rabais)
                    
                    elif dataRecherche.iloc[0]["UnitCostOnDemandNegocie"]>0:    
                        # print("Test2")
                        result= dataRecherche.iloc[0]["UnitCostOnDemandNegocie"]
                        
                elif not negocie and dataRecherche.iloc[0]["UnitCostOnDemand"]!=None and dataRecherche.iloc[0]["UnitCostOnDemand"]>0:
                    
                    result= dataRecherche.iloc[0]["UnitCostOnDemand"]
                        
                unit_cost_ondemand_nego=get_unit_cost_on_demand(data_source,dataRecherche.iloc[0],service,negocie)
                unit_cost_ondemand_non_nego=get_unit_cost_on_demand(data_source,dataRecherche.iloc[0],service,not negocie)
                
                if unit_cost_ondemand_nego==unit_cost_ondemand_non_nego and row["Date"]>="2023-09-01" and negocie:
                    result= unit_cost_ondemand_non_nego*(1-rabais)
                else:
                    result= unit_cost_ondemand_nego
                
            else :
                result =0.0
    return result

# %%
# AZURE APP SERVICE UNIT COSTS

def get_other_unit_cost_on_demand(data_source,row):
    data=data_source[(data_source["ProjectName"]==row["ProjectName"])&(data_source["LocationName"]==row["LocationName"])&(data_source["ServiceName"]==row["ServiceName"])&(data_source["CostOnDemand"]>0)&(data_source["UsageOnDemand"]>0)&(data_source["ResourceSku"]==row["ResourceSku"][0])].sort_values("Date",ascending=False)
    # data["ResourceSku"]=data.apply(lambda row:row["ResourceSkuTier"]+" "+row["ResourceSkuSize"],axis=1)

    if data.empty:
        data=data_source[(data_source["ProjectName"]!="IT-DEV")&(data_source["LocationName"]==row["LocationName"])&(data_source["ServiceName"]==row["ServiceName"])&(data_source["CostOnDemand"]>0)&(data_source["UsageOnDemand"]>0)&(data_source["ResourceSku"]==row["ResourceSku"][0])].sort_values("Date",ascending=False)

    if data.empty:
        data=data_source[(data_source["LocationName"]==row["LocationName"])&(data_source["ServiceName"]==row["ServiceName"])&(data_source["CostOnDemand"]>0)&(data_source["UsageOnDemand"]>0)&(data_source["ResourceSku"]==row["ResourceSku"][0])].sort_values("Date",ascending=False)

    if not data.empty:
        somme=0

        for i in range(len(data)):
            row=data.iloc[i]
            # print("Test2 ")
            if i>20:
                return(somme/21)

            somme+=get_unit_cost_on_demand_v2(data_source,row,service=row["ServiceName"])[0]
        return somme/len(data)
    return 0.0
# print(somme/len(test))
# print(row["Date"],row["ResourceSku"],row["UsageOnDemand"],row["CostOnDemand"],(get_unit_cost_on_demand_v2(resourcesCoreModel,row,service="SQL Database")))

def get_precedent_unit_cost(data_source,row,service,negocie):
    i=0
    result=0
    # print(row["Date"],"Recherche précédent")
    dataRecherche=data_source.copy()
    dataRecherche=dataRecherche[(dataRecherche["Date"]<row["Date"])&(dataRecherche["UsageOnDemand"]>0)&(dataRecherche["ServiceName"]==service)&(dataRecherche["ResourceGroupName"]==row["ResourceGroupName"])&(dataRecherche["ResourceName"]==row["ResourceName"])]
    dataRecherche = dataRecherche[dataRecherche["UsageOnDemand"]>0]
    # return dataRecherche
    
    if dataRecherche.empty:
        dataRecherche=data_source.copy()
        dataRecherche=dataRecherche[(dataRecherche["UsageOnDemand"]>0)&(dataRecherche["ServiceName"]==service)&(dataRecherche["ResourceGroupName"]==row["ResourceGroupName"])&(dataRecherche["ResourceName"]==row["ResourceName"])]
        dataRecherche = dataRecherche[dataRecherche["UsageOnDemand"]>0]

    if not dataRecherche.empty:
        dataRecherche = dataRecherche.sort_values(by="Date", ascending=False)
        dataRef= dataRecherche[dataRecherche["Date"]<"2023-09-01"].copy().sort_values("Date",ascending=False)

        rabais=0.11
        while result ==0.0 and i<len(dataRecherche):
            ref = 0
            j=0
            
            while j<len(dataRef):
                rowReferenceOnDemand=dataRef.iloc[j].copy()
                if rowReferenceOnDemand["UsageOnDemand"]>1 and rowReferenceOnDemand["CostOnDemand"]>0.1:
                    ref = (rowReferenceOnDemand["CostOnDemand"]/rowReferenceOnDemand["UsageOnDemand"])
                    break
                j+=1

            # print(dataRecherche.iloc[i])
            if dataRecherche.iloc[i]["Date"]>="2023-09-01" and dataRecherche.iloc[i]["UsageOnDemand"]>1 and dataRecherche.iloc[i]["CostOnDemand"]>0.1:
                # print(negocie,"Test 3")
                if ref>0:
                    rabais= 1-dataRecherche["CostOnDemand"].iloc[i]/dataRecherche["UsageOnDemand"].iloc[i]/ref

                # print(ref,rabais)

                result= dataRecherche["CostOnDemand"].iloc[i]/dataRecherche["UsageOnDemand"].iloc[i]/(1.0-rabais)
                break
                
            elif dataRecherche.iloc[i]["UsageOnDemand"]>1 and dataRecherche.iloc[i]["CostOnDemand"]>0.1:    
                # print(negocie,"Test 4")
                result= dataRecherche.iloc[i]["CostOnDemand"]/dataRecherche.iloc[i]["UsageOnDemand"]
                # return  dataRecherche.iloc[i]
                break
            
            # print(result)
            i+=1
    else :
        # print("get other")
        result =get_other_unit_cost_on_demand(data_source,row)
        # facteur=0.0
    return result


def get_unit_cost_on_demand_v2(data_source,row, service,negocie=True):
    # print(row["ResourceName"])
    result=0.0
    rabais = 0.11
    # facteur=1.0
    i=0

    row["Capacité"] = 0 
    # if not service=="SQL Database":
    #     row = row.copy()
    #     row["Capacité"] = 0 
 
    if not "UnitCostOnDemand" in data_source.columns:
        data_source["UnitCostOnDemand"]=0.0
    
    if service in ["Azure App Service","Virtual Machines"]:
        
        if row["UsageOnDemand"]>1 and row["CostOnDemand"]>0.1: 
            
            if row["Date"]>="2023-09-01":#Après négociation microsoft
                
                # print("Test",data_source[(data_source["ResourceName"]==row["ResourceName"])&(data_source["Date"]<"2023-09-01")]["ResourceSkuTier"])
                # return data_source[(data_source["LocationName"]==row["LocationName"])&(data_source["ResourceName"]==row["ResourceName"])&(data_source["ResourceGroupName"]==row["ResourceGroupName"])&(data_source["ResourceSkuTier"]+" "+data_source["ResourceSkuSize"]==row["ResourceSku"])&(data_source["Date"]<row["Date"])].sort_values("Date",ascending=False)
                # print("Test")
                previous_data=data_source[(data_source["LocationName"]==row["LocationName"])&(data_source["ResourceName"]==row["ResourceName"])&(data_source["ResourceGroupName"]==row["ResourceGroupName"])&(data_source["ResourceSkuTier"]+" "+data_source["ResourceSkuSize"]==row["ResourceSku"][0])&(data_source["Date"]<"2023-09-01")].sort_values("Date",ascending=False)
                            # return data_source[(data_source["LocationName"]==row["LocationName"])&(data_source["ResourceName"]==row["ResourceName"])&(data_source["ResourceGroupName"]==row["ResourceGroupName"])&(data_source["ResourceSkuTier"]+" "+data_source["ResourceSkuSize"]==row["ResourceSku"][0])&(data_source["Date"]<row["Date"])]

                j=0
                last_unit_on_demand=0
                
                
                while j<len(previous_data) and last_unit_on_demand==0:
                    if previous_data["UsageOnDemand"].iloc[j]>1 and previous_data["CostOnDemand"].iloc[j]>0.1 and previous_data["CostOnDemand"].iloc[j]/previous_data["UsageOnDemand"].iloc[j]>0:
                        
                        
                    
                        last_unit_on_demand=previous_data["CostOnDemand"].iloc[j]/previous_data["UsageOnDemand"].iloc[j]
                        # if row["Date"]=="2023-09-01":
                        
                            # print(previous_data["Date"].iloc[j],previous_data["CostOnDemand"].iloc[j],previous_data["UsageOnDemand"].iloc[j],previous_data["CostOnDemand"].iloc[j])
                            # print(last_unit_on_demand)
                    j+=1
                    
                # print(last_unit_on_demand)
                if last_unit_on_demand==0 :
                    # print("Test")

                    # print(negocie,"Test 1")
                    result= row["CostOnDemand"]/row["UsageOnDemand"]/(row["Capacité"]+1)/(1-rabais)

                   

                else:
                    result= last_unit_on_demand
                # facteur=1-rabais
            else:
                # print(negocie,"Test 2")
                result= row["CostOnDemand"]/row["UsageOnDemand"]
                # facteur=1.0

        else:#rechercher précédent
            # dataRecherche=resourcesAzure.copy()
            
            # print("Recherche Précédent")
            result = get_precedent_unit_cost(data_source,row,service,negocie)

    if service=="SQL Database":
        lignesFacturationSQL = search_lignes_facturations(lignesFacturationCoreModel.copy(),"SQL Database",row["ResourcePath"],date=row["Date"])
        result = get_unit_costs_sql_database(lignesFacturationSQL)[1]

        facteur = 1.0

        if row["Date"]>="2023-09-01":
            result=result/(1-rabais)
            facteur = (1- rabais)



    else:
        if row["Date"]>="2023-09-01":
            if row["UsageOnDemand"]>0 and result>0:
                facteur=row["CostOnDemand"]/row["UsageOnDemand"]/result
            else: 
                facteur=1-rabais
        else:
            facteur=1.0
    print(negocie,row["Date"],result,facteur)
    return (result,facteur)


# nees=resourcesCoreModel.copy()
# date_min="2022-10-01"
# date_max="2023-12-01"
# resourceName="sqldb-cn-cmss-prod01-cmss"
# test=get_gains_totaux_sql_database(donnees,date_min,date_max,gains_premier_mois=False,resourceName=resourceName)
# # %store gains_tot_sql_database_CoreModel

# test.to_csv("C:\\Users\\hugo.dufrene\\Wavestone\\WE - ROLEX [CH] - Support mise en place Gouvernance Cloud public - 13665 - Documents partages\\Gouvernance Cloud\\FinOps\\Optimisations\\SQL Database\\20240116 - Analyse Gains Optims SQL Database.csv",index=False)

# test[["Date","ResourceName","Instances","ResourceSku","UsageTot","UsageOnDemand","CostOnDemand","CostReservation1Y","UnitCostOnDemand","UnitCostOnDemandNegocie","CostJ","VariationJ","GainJ","CostJI","VariationJI","GainJI","GainFonctionalitesSkuJI","GainChangementSkuJI","GainReservation1YJI","GainContratsJI","GainSizingJ","GainDecommissionnementJ","VariationTot","GainTot"]]



# %%
#VIRTUAL MACHINES


def get_other_unit_cost_on_demand(data_source,row):
    data=data_source[(data_source["ProjectName"]==row["ProjectName"])&(data_source["LocationName"]==row["LocationName"])&(data_source["ServiceName"]==row["ServiceName"])&(data_source["CostOnDemand"]>0)&(data_source["UsageOnDemand"]>0)&(data_source["ResourceSku"]==row["ResourceSku"][0])].sort_values("Date",ascending=False)
    # data["ResourceSku"]=data.apply(lambda row:row["ResourceSkuTier"]+" "+row["ResourceSkuSize"],axis=1)

    if data.empty:
        data=data_source[(data_source["ProjectName"]!="IT-DEV")&(data_source["LocationName"]==row["LocationName"])&(data_source["ServiceName"]==row["ServiceName"])&(data_source["CostOnDemand"]>0)&(data_source["UsageOnDemand"]>0)&(data_source["ResourceSku"]==row["ResourceSku"][0])].sort_values("Date",ascending=False)

    if data.empty:
        data=data_source[(data_source["LocationName"]==row["LocationName"])&(data_source["ServiceName"]==row["ServiceName"])&(data_source["CostOnDemand"]>0)&(data_source["UsageOnDemand"]>0)&(data_source["ResourceSku"]==row["ResourceSku"][0])].sort_values("Date",ascending=False)

    if not data.empty:

        # test=resourcesCoreModel[(resourcesCoreModel["ProjectName"]!="IT-DEV")&(resourcesCoreModel["Date"]>="2021-01-01")&(resourcesCoreModel["LocationName"]==rowTest["LocationName"])&(resourcesCoreModel["ServiceName"]==rowTest["ServiceName"])&(resourcesCoreModel["ServiceTier"]==rowTest["ServiceTier"])&(resourcesCoreModel["CostOnDemand"]>0)&(resourcesCoreModel["UsageOnDemand"]>0)&(resourcesCoreModel["ResourceSkuSize"]==rowTest["ResourceSkuSize"])].sort_values("Date",ascending=False)
        # test["ResourceSku"]=test.apply(lambda row:row["ResourceSkuTier"]+" "+row["ResourceSkuSize"],axis=1)
        somme=0

        for i in range(len(data)):
            row=data.iloc[i]
            # print("Test2 ")
            if i>20:
                return(somme/21)

            somme+=get_unit_cost_on_demand_v2(data_source,row,service=row["ServiceName"])[0]
        return somme/len(data)
    return 0.0
# print(somme/len(test))
# print(row["Date"],row["ResourceSku"],row["UsageOnDemand"],row["CostOnDemand"],(get_unit_cost_on_demand_v2(resourcesCoreModel,row,service="SQL Database")))

def get_precedent_unit_cost(data_source,row,service,negocie):
    i=0
    result=0
    # print(row["Date"],"Recherche précédent")
    dataRecherche=data_source.copy()
    dataRecherche=dataRecherche[(dataRecherche["Date"]<row["Date"])&(dataRecherche["UsageOnDemand"]>0)&(dataRecherche["ServiceName"]==service)&(dataRecherche["ResourceGroupName"]==row["ResourceGroupName"])&(dataRecherche["ResourceName"]==row["ResourceName"])]
    dataRecherche = dataRecherche[dataRecherche["UsageOnDemand"]>0]
    # return dataRecherche
    
    if dataRecherche.empty:
        dataRecherche=data_source.copy()
        dataRecherche=dataRecherche[(dataRecherche["UsageOnDemand"]>0)&(dataRecherche["ServiceName"]==service)&(dataRecherche["ResourceGroupName"]==row["ResourceGroupName"])&(dataRecherche["ResourceName"]==row["ResourceName"])]
        dataRecherche = dataRecherche[dataRecherche["UsageOnDemand"]>0]



    if not dataRecherche.empty:
        dataRecherche = dataRecherche.sort_values(by="Date", ascending=False)
        dataRef= dataRecherche[dataRecherche["Date"]<"2023-09-01"].copy().sort_values("Date",ascending=False)

        rabais=0.12
        while result ==0.0 and i<len(dataRecherche):
            ref = 0
            j=0
            
            while j<len(dataRef):
                rowReferenceOnDemand=dataRef.iloc[j].copy()
                if rowReferenceOnDemand["UsageOnDemand"]>=1 and rowReferenceOnDemand["CostOnDemand"]>=0.1:
                    ref = (rowReferenceOnDemand["CostOnDemand"]/rowReferenceOnDemand["UsageOnDemand"])
                    break
                j+=1




            # print(dataRecherche.iloc[i])
            if dataRecherche.iloc[i]["Date"]>="2023-09-01" and dataRecherche.iloc[i]["UsageOnDemand"]>=1 and dataRecherche.iloc[i]["CostOnDemand"]>=0.1:
                print(negocie,"Test 3")
                if ref>0:
                    rabais= 1-dataRecherche["CostOnDemand"].iloc[i]/dataRecherche["UsageOnDemand"].iloc[i]/ref

                print(ref,rabais)

                result= dataRecherche["CostOnDemand"].iloc[i]/dataRecherche["UsageOnDemand"].iloc[i]/(1.0-rabais)
                break
                
            elif dataRecherche.iloc[i]["UsageOnDemand"]>=1 and dataRecherche.iloc[i]["CostOnDemand"]>=0.1:    
                print(negocie,"Test 4")
                result= dataRecherche.iloc[i]["CostOnDemand"]/dataRecherche.iloc[i]["UsageOnDemand"]
                # return  dataRecherche.iloc[i]
                break
            
            # print(result)
            i+=1
    else :
        print("get other")
        result =get_other_unit_cost_on_demand(data_source,row)
        # facteur=0.0
    return result


def get_unit_cost_on_demand_v2(data_source,row, service,negocie=True):
    # print(row)
    result=0.0
    rabais = 0.11
    # facteur=1.0
    i=0

    # row["ResourceSku"]=row["ResourceSkuTier"]+" "+row["ResourceSkuSize"]
    if service=="SQL Database":
        row = row.copy()
        row["Capacité"] = row["UsageTot"]/(row["vCore"]*24*row["NombreJoursMois"])
    else:
        row["Capacité"]=1
 
    if not "UnitCostOnDemand" in data_source.columns:
        data_source["UnitCostOnDemand"]=0.0
    
    if service in ["Azure App Service","Virtual Machines","SQL Database"]:
        
        if row["UsageOnDemand"]>=1 and row["CostOnDemand"]>=0.1: 
            
            if row["Date"]>="2023-09-01":#Après négociation microsoft
                
                # print("Test",data_source[(data_source["ResourceName"]==row["ResourceName"])&(data_source["Date"]<"2023-09-01")]["ResourceSkuTier"])
                # return data_source[(data_source["LocationName"]==row["LocationName"])&(data_source["ResourceName"]==row["ResourceName"])&(data_source["ResourceGroupName"]==row["ResourceGroupName"])&(data_source["ResourceSkuTier"]+" "+data_source["ResourceSkuSize"]==row["ResourceSku"])&(data_source["Date"]<row["Date"])].sort_values("Date",ascending=False)
                # print("Test")
                previous_data=data_source[(data_source["LocationName"]==row["LocationName"])&(data_source["ResourceName"]==row["ResourceName"])&(data_source["ResourceGroupName"]==row["ResourceGroupName"])&(data_source["ResourceSkuTier"]+" "+data_source["ResourceSkuSize"]==row["ResourceSku"][0])&(data_source["Date"]<"2023-09-01")].sort_values("Date",ascending=False)
                            # return data_source[(data_source["LocationName"]==row["LocationName"])&(data_source["ResourceName"]==row["ResourceName"])&(data_source["ResourceGroupName"]==row["ResourceGroupName"])&(data_source["ResourceSkuTier"]+" "+data_source["ResourceSkuSize"]==row["ResourceSku"][0])&(data_source["Date"]<row["Date"])]
                
                
                # previous_data["UsageOnDemand"] =previous_data.apply(lambda rowt:rowt["UsageOnDemand"] * 24 if "DTU" in rowt["ResourceSkuSize"] else rowt["UsageOnDemand"]/max(rowt["Capacité"],1),axis=1)
                # previous_data["UsageOnDemand"] =previous_data.apply(lambda row:row["UsageTot"] * 24 if "DTU" in row["ResourceSkuSize"] else row["UsageTot"]/max(row["Capacité"],1),axis=1)
                # print(previous_data["UsageOnDemand"].iloc[0],previous_data["ResourceSkuSize"].iloc[0],previous_data["Capacité"].iloc[0])

                j=0
                last_unit_on_demand=0
                
                
                while j<len(previous_data) and last_unit_on_demand==0:
                    if previous_data["UsageOnDemand"].iloc[j]>1 and previous_data["CostOnDemand"].iloc[j]>0.1 and previous_data["CostOnDemand"].iloc[j]/previous_data["UsageOnDemand"].iloc[j]>0:
                        
                        
                    
                        last_unit_on_demand=previous_data["CostOnDemand"].iloc[j]/previous_data["UsageOnDemand"].iloc[j]
                        # if row["Date"]=="2023-09-01":
                        
                            # print(previous_data["Date"].iloc[j],previous_data["CostOnDemand"].iloc[j],previous_data["UsageOnDemand"].iloc[j],previous_data["CostOnDemand"].iloc[j])
                            # print(last_unit_on_demand)
                    j+=1
                    
                # print(last_unit_on_demand)
                if last_unit_on_demand==0 : 
                    result= row["CostOnDemand"]/row["UsageOnDemand"]/(1-rabais)

    

                else:
                    result= last_unit_on_demand
                # facteur=1-rabais
            else:
                # print(negocie,"Test 2")
                result= row["CostOnDemand"]/row["UsageOnDemand"]
                # facteur=1.0

        else:#rechercher précédent
            # dataRecherche=resourcesAzure.copy()
            
            print("Recherche Précédent")
            result = get_precedent_unit_cost(data_source,row,service,negocie)
            
    
    # if not "DTU" in row["ResourceSkuSize"]:
    #     result=result/24
        
    if row["Date"]>="2023-09-01":
        if row["UsageOnDemand"]>0 and result>0:
            facteur=row["CostOnDemand"]/row["UsageOnDemand"]/result
        else: 
            facteur=1-rabais
    else:
        facteur=1.0
    # print(negocie,row["Date"],result,facteur)
    return (result,facteur)

# %%
def get_nb_instances(service,row,tolerance=0.1,brut=True):
    # Vérifier que la marge d'erreur est positive
    # print(row)
    
    date_str=row["Date"]
    jours=get_count_days(date_str=date_str)
    usage=row["UsageTot"]
    # print(usage)
    heures=jours*24
    row["Capacité"]=0
    # return usage/jours
    if service =="SQL Database":
        # if "DTU" in row["ResourceSkuSize"]:
        if (row["Config"]=="Compute" or row["Config"]=="Licence") and not "DTU" in row["ResourceSkuSize"]:
            nb_instances=usage/heures/max(row["Capacité"],1)
            # if not brut:
                # nb_instances=usage/heures

                # if row["ResourceSkuSize"]=="vCore":
                #     vCore =  get_nb_vcore(row)
                # else:
                #     vCore=None

                # if not vCore==None and vCore>0:
                #     nb_instances=usage/heures
                # else:
                #     nb_instances=usage/heures
                #     if row["ResourceSkuSize"]=="vCore":
                #         print(row["ResourceName"],vCore)
                # nb_instances=usage/heures
            # else:
            #     nb_instances=usage/heures
        else:
            nb_instances=usage/jours
        # else:
        #     nb_instances=usage/heures
    else:
        nb_instances=usage/heures
    # print(nb_instances)
    return nb_instances
    
    calcul=abs(nb_instances - round(nb_instances))
    
    if calcul>0.5:
        calcul=1-calcul
        
    
    
    if calcul< tolerance:
        nb_instances_arrondi = round(nb_instances)
        tolerance=calcul
        # print(nb_instances_arrondi,calcul,tolerance)
        return nb_instances_arrondi
        
    else:
        # print("Erreur",date_str,usage,tolerance,"=> ",nb_instances)
        return nb_instances
    
# test = get_gains_totaux_sql_database(donnees,date_min,date_max,gain_contrat=True)

# test[test["Config"]=="Compute"]

# test[["Date","Instances","Capacité","ResourceSku","UsageTot","UsageOnDemand","UsageReservation1Y","CostTot","VariationTot","GainTot","UnitCostOnDemand","CostJI","GainJI","VariationJI","GainChangementSkuJI","GainFonctionalitesSkuJI","GainReservation1YJI","GainContratsJI","GainSizingJ","GainDecommissionnementJ","VariationJ","GainJ","CostJ"]]


# %%
def search_lignes_facturations(lignesFacturation,service,resourceId,date=None):
    df_test=pd.DataFrame()

    for ligne in lignesFacturation:
        if ligne["Service"]==service and ligne["ResourceId"]==resourceId and (date==None or ligne["Date"]==date):
            df_test=pd.concat([df_test,pd.DataFrame([ligne])])

    return df_test.sort_values("Date")



def get_nb_vcore(lignesFacturation):
    nbJours = get_count_days(lignesFacturation["Date"].iloc[0])
    data_DTU = lignesFacturation[(lignesFacturation["MeterName"].str.contains("DTU"))]
    if not data_DTU.empty:
        hDTU = data_DTU["Usage"].sum()*24
    else:
        hDTU=0
    # return hDTU*24
    data_vCore = lignesFacturation[(lignesFacturation["MeterName"]=="vCore")&(lignesFacturation["MeterCategory"].str.contains("Compute"))&(~lignesFacturation["MeterCategory"].str.contains("License"))]
    
    if not data_vCore.empty:
        # print(lignesFacturation)
        # print("Usage",data_vCore["Usage"].sum(),hDTU)
        return max(round(data_vCore["Usage"].sum()/(24*nbJours-hDTU)),2)
    return 1
    # return (data_vCore["Usage"].sum()- hDTU*24)/nbJours/24

# path  = '/subscriptions/669420be-9749-47c9-be7e-8b7c6f042a25/resourcegroups/rg-xd-cmss-sfcm/providers/microsoft.sql/servers/sql-xd-wsa-alm/databases/sqldb_alm_wsa'
# lignesFacturation = search_lignes_facturations(lignesFacturationCoreModel.copy(),"SQL Database",path,date="2023-07-01")
# lignesFacturation
# # get_nb_vcore(lignesFacturation)

# %%
def get_unit_costs_sql_database(lignesFacturation,reservation=False):
    nbVCore = get_nb_vcore(lignesFacturation)
    nbJours = get_count_days(lignesFacturation["Date"].iloc[0])
    usageTotOD=0
    usageTotR1Y=0
    usageTot=0
    coutDTU=0
    coutvCore=0
    # return lignesFacturation[(lignesFacturation["MeterName"]=="vCore")&(lignesFacturation["MeterCategory"].str.contains("Compute"))&(~lignesFacturation["MeterCategory"].str.contains("License"))]

    sommeDTU=0

    lignesFacturation[(lignesFacturation["MeterName"]=="vCore")&(lignesFacturation["MeterCategory"].str.contains("Compute"))&(~lignesFacturation["MeterCategory"].str.contains("License"))&(lignesFacturation["PricingModel"]=="Reservation")]

    
    
    data_vCoreR1Y=lignesFacturation[(lignesFacturation["MeterName"]=="vCore")&(lignesFacturation["MeterCategory"].str.contains("Compute"))&(~lignesFacturation["MeterCategory"].str.contains("License"))&(lignesFacturation["PricingModel"]=="Reservation")]
    
    if not data_vCoreR1Y.empty:
        # data_vCoreR1Y = data_vCoreR1Y.iloc[0]
        coutvCoreR1Y=data_vCoreR1Y["Coût"].sum()
        usageTotR1Y+=data_vCoreR1Y["Usage"].sum()/nbVCore
    

    data_vCoreOD=lignesFacturation[(lignesFacturation["MeterName"]=="vCore")&(lignesFacturation["MeterCategory"].str.contains("Compute"))&(~lignesFacturation["MeterCategory"].str.contains("License"))&(lignesFacturation["PricingModel"]=="OnDemand")]
    
    if not data_vCoreOD.empty:
        # data_vCoreOD = data_vCoreOD.iloc[0]
        coutvCore=data_vCoreOD["Coût"].sum()
        # print(nbVCore)
        usageTotOD+=data_vCoreOD["Usage"].sum()/nbVCore

    # print(data_vCoreOD["Coût"],usageTotOD,data_vCoreOD["Coût"]/usageTotOD)
    # return data_vCoreOD["Coût"]/data_vCoreOD["Usage"]*nbVCore

    data_DTU = lignesFacturation[(lignesFacturation["MeterName"].str.contains("DTU"))]
    # return data_DTU

    if not data_DTU.empty:
        
        sommeDTU=data_DTU["Coût"].sum()
        usageTotOD+=data_DTU["Usage"].sum()*24    



    if reservation:
        if usageTotR1Y>0:
            return ( nbVCore,coutvCoreR1Y/(usageTotR1Y)) #*nbJours*24/(usageTotOD+usageTotR1Y))
    else:
        if usageTotOD>0 and nbJours>0:
            # return ( nbVCore,(sommeDTU+coutvCore)/(usageTotOD)*(usageTotOD+usageTotR1Y)/(nbVCore*24*nbJours))
            return ( nbVCore,(sommeDTU+coutvCore)/(usageTotOD))
        
    return( nbVCore,0)

# lignesFacturation = search_lignes_facturations(lignesFacturationCoreModel.copy(),"SQL Database",'/subscriptions/37687f29-0ead-4d57-aefc-91d8e2cbe452/resourcegroups/rg-cn-cmss-prod/providers/microsoft.sql/servers/sql-cn-cmss-prod01/databases/sqldb-cn-cmss-prod01-cmss',date="2023-12-01")
# get_unit_costs_sql_database(lignesFacturation,reservation=False)

# %%
def get_couts_license_sql_database(datagrouped,row_parcourue):
    dfCoutLicence= datagrouped[(datagrouped["Config"]=="Licence")&(datagrouped["ResourceName"]==row_parcourue["ResourceName"])&(datagrouped["ResourceGroupName"]==row_parcourue["ResourceGroupName"])&(datagrouped["ResourcePath"]==row_parcourue["ResourcePath"])&(datagrouped["Date"]==row_parcourue["Date"])]
    rabais = 0.11

    if not dfCoutLicence.empty:
        sku = dfCoutLicence["ResourceSku"].iloc[0][0]
        sku=sku.split("-")[0]+"- SQL License"

        if  azure_prices[(row_parcourue["ProjectId"]==azure_prices["ProjectId"])].empty:
            
            if dfCoutLicence["CostTot"].iloc[0]>0 and dfCoutLicence["UsageTot"].iloc[0]>0:

                if row_parcourue["Date"]>="2023-09-01":
                    return(dfCoutLicence["CostTot"].iloc[0]/dfCoutLicence["UsageTot"].iloc[0],dfCoutLicence["CostTot"].iloc[0]/dfCoutLicence["UsageTot"].iloc[0]/(1-rabais),dfCoutLicence["CostTot"].iloc[0]/dfCoutLicence["UsageTot"].iloc[0])

                else:
                    return(dfCoutLicence["CostTot"].iloc[0]/dfCoutLicence["UsageTot"].iloc[0],dfCoutLicence["CostTot"].iloc[0]/dfCoutLicence["UsageTot"].iloc[0],dfCoutLicence["CostTot"].iloc[0]/dfCoutLicence["UsageTot"].iloc[0])


        
        else:
            if row_parcourue["Date"]>="2023-09-01":        
                coutLicenceNonNegoJI = azure_prices[(row_parcourue["ProjectId"]==azure_prices["ProjectId"])&(~azure_prices["meterSubCategory"].isna())&(azure_prices["meterName"]=="vCore")&(azure_prices["meterSubCategory"]==sku)&(azure_prices["unitPrice"]>0)&(azure_prices["Date"]=="2023-08-01")].sort_values("Date",ascending=False)["unitPrice"].iloc[0]
                coutLicenceNegoJI = azure_prices[(row_parcourue["ProjectId"]==azure_prices["ProjectId"])&(~azure_prices["meterSubCategory"].isna())&(azure_prices["meterName"]=="vCore")&(azure_prices["meterSubCategory"]==sku)&(azure_prices["Date"]==row_parcourue["Date"])&(azure_prices["unitPrice"]>0)].sort_values("Date")
                if coutLicenceNegoJI.empty:
                    coutLicenceNegoJI=coutLicenceNonNegoJI
                else :
                    coutLicenceNegoJI=coutLicenceNegoJI["unitPrice"].iloc[0]
                # print(coutLicenceNegoJI)
                return(dfCoutLicence["CostTot"].iloc[0]/dfCoutLicence["UsageTot"].iloc[0],coutLicenceNonNegoJI/100*row_parcourue["vCore"],coutLicenceNegoJI/100*row_parcourue["vCore"])
            
            else:
                coutLicenceNegoJI = azure_prices[(row_parcourue["ProjectId"]==azure_prices["ProjectId"])&(~azure_prices["meterSubCategory"].isna())&(azure_prices["meterName"]=="vCore")&(azure_prices["meterSubCategory"]==sku)&(azure_prices["Date"]==row_parcourue["Date"])&(azure_prices["unitPrice"]>0)].sort_values("Date")
                if coutLicenceNegoJI.empty:
                    coutLicenceNegoJI=0.0
                else :
                    coutLicenceNegoJI=coutLicenceNegoJI["unitPrice"].iloc[0]
            
                return(dfCoutLicence["CostTot"].iloc[0]/dfCoutLicence["UsageTot"].iloc[0],coutLicenceNegoJI/100*row_parcourue["vCore"],coutLicenceNegoJI/100*row_parcourue["vCore"])


    # return  dfCoutLicence["CostTot"].sum()/row_parcourue["NombreJoursMois"]
    return (0.0,0.0,0.0)

# %%
def populate_costJLicence_sql_database(ligne_licence):
    if ligne_licence["Config"]=="Licence":
        return (
                ligne_licence["UsageTot"],
                ligne_licence["CostTot"]/ligne_licence["NombreJoursMois"],
                ligne_licence["UsageTot"]*ligne_licence["UnitCostJLicenceTheo"]/ligne_licence["NombreJoursMois"],
                ligne_licence["UsageTot"]*ligne_licence["UnitCostJLicenceTheoNego"]/ligne_licence["NombreJoursMois"])

    else:
        
        return (0.0,0.0,0.,0.)
    

def populate_costJStockage_sql_database(datagrouped,ligne):
    coutStockageJ=0.0
    facteur=0.11
    if ligne["Config"]=="Storage":
        dfCoutStockage=datagrouped[(datagrouped["Config"]=="Storage")&(datagrouped["ResourceName"]==ligne["ResourceName"])&(datagrouped["ResourceGroupName"]==ligne["ResourceGroupName"])&(datagrouped["ResourcePath"]==ligne["ResourcePath"])&(datagrouped["Date"]==ligne["Date"])]

        if not dfCoutStockage.empty:
            coutStockageJ=dfCoutStockage["CostTot"].sum()/ligne["NombreJoursMois"]          

    if ligne["Date"]>="2023-09-01":
        return (coutStockageJ/(1-facteur),(1-facteur))
    
    else:
        return (coutStockageJ,1.0)





# %%
def get_gains_totaux_sql_database(donnees,date_min,date_max,gain_contrat=True,gains_premier_mois=True,resourceName=None):
     # date_max=sorted(list(donnees["Date"].unique()))[-1]

    donnees=donnees[donnees["ServiceName"]=="SQL Database"]
    
    if not resourceName==None:
        donnees=donnees[(donnees["ResourceName"]==resourceName)]
    # gains_tot_sql_database_CoreModel[gains_tot_sql_database_CoreModel["ResourceName"]=="xd-cmss-dev"]
    # donnees=donnees[(donnees["ProjectName"]=='IT-PROD-IT')]
    
    data=donnees.groupby(["Date","ResourceName","ProjectId","ProjectName","ApplicationName","LocationName","ServiceName","ResourceGroupName","ResourcePath","ServiceTier","ResourceSkuTier","ResourceSkuSize"])[["UsageTot","UsageOnDemand","CostTot","CostOnDemand","CostReservation1Y","UsageReservation1Y"]].sum().reset_index()    

    
    # return data
    gains=None
 
    data = data.fillna(0)
    # return data
    data["Config"] = data.apply(lambda row: get_config(row), axis=1)
    # data["Capacité"]=data.apply(lambda row:get_nb_vcore(row),axis=1)
    # return data
    data["ResourceSku"]=data.apply(lambda row:row["ResourceSkuTier"]+" "+row["ResourceSkuSize"],axis=1)
    
    data["UsageReservation1YBrut"]=data["UsageReservation1Y"].copy()
    data["UsageOnDemandBrut"]=data["UsageOnDemand"].copy()
    data["UsageTotBrut"]=data["UsageTot"].copy()

    data[["vCore","UnitCostReservation1Y"]]=data.apply(lambda row :pd.Series (get_unit_costs_sql_database(search_lignes_facturations(lignesFacturationCoreModel.copy(),"SQL Database",row["ResourcePath"],date=row["Date"]),reservation=True)) ,axis=1)
    data["UsageTot"]=data.apply(lambda row:row["UsageTot"] * 24 if "DTU" in row["ResourceSkuSize"] else row["UsageTot"]/row["vCore"],axis=1)
    data["UsageOnDemand"] = data.apply(lambda row:row["UsageOnDemand"] * 24 if "DTU" in row["ResourceSkuSize"] else row["UsageOnDemand"]/row["vCore"],axis=1)
    data["UsageReservation1Y"] = data.apply(lambda row:row["UsageReservation1Y"] * 24 if "DTU" in row["ResourceSkuSize"] else row["UsageReservation1Y"]/row["vCore"],axis=1)
    # data[]=data.apply(lambda row:get_nb_instances("SQL Database",row,brut=True),axis=1)


    data_source=data.copy()
    # data_source = donnees.groupby(["Date","ProjectName","ServiceName","ResourceName"])[["CostTot","UsageTot","CostOnDemand","UsageOnDemand","CostReservation1Y","UsageReservation1Y"]].sum().reset_index()
    data_source["UnitCostOnDemand"]=0.0
    data_source["UnitCostOnDemandNegocie"]=0.0
    
    data=data[data["Date"]>=date_min]
        
    data["InstancesBrut"]=1
    data["Instances"]=1

    # return data

    # data_source["UsageTot"] =data_source.apply(lambda row:row["UsageTot"] * 24 if "DTU" in row["ResourceSkuSize"] else row["UsageTot"]/max(row["Capacité"],1),axis=1)
    # data_source["UsageOnDemand"] =data_source.apply(lambda row  :row["UsageOnDemand"] * 24 if "DTU" in row["ResourceSkuSize"] else row["UsageOnDemand"]/max(row["Capacité"],1),axis=1)
    # return data
    
    datagrouped=data.groupby(["Date","ResourceName","ProjectId","ProjectName","ApplicationName","LocationName","ServiceName","ResourceGroupName","ResourcePath","Config"])[["UsageTot","CostTot","UsageOnDemand","UsageOnDemandBrut","CostOnDemand","CostReservation1Y","UsageReservation1Y","UsageReservation1YBrut","Instances","InstancesBrut"]].sum().reset_index()

    datagrouped["ResourceSku"] = datagrouped.apply(lambda row: list(data[
        (data["Date"] == row["Date"]) &
        (data["ResourceName"] == row["ResourceName"])&(data["Config"]==row["Config"])    ]["ResourceSku"].unique()), axis=1) 
        
    datagrouped["InstancesBrut"]=1
    datagrouped["Instances"]=1


    datagrouped["UnitCostOnDemand"]=0.
    datagrouped["UnitCostOnDemandNegocie"]=0.
    datagrouped["UnitCostR1Y"]=0.
    # return data_source
    
    print("Pré traitement 1 terminé")
    # return datagrouped
    datagrouped["NombreJoursMois"]=datagrouped["Date"].apply(lambda row:get_count_days(row))
    datagrouped[["vCore","UnitCostReservation1Y"]]=datagrouped.apply(lambda row :pd.Series (get_unit_costs_sql_database(search_lignes_facturations(lignesFacturationCoreModel.copy(),"SQL Database",row["ResourcePath"],date=row["Date"]),reservation=True)) ,axis=1)

    datagrouped[["UnitCostOnDemand", "FacteurRabaisNegociationContrat"]] = datagrouped.apply(
        lambda row: pd.Series(get_unit_cost_on_demand_v2(data_source, row, "SQL Database", negocie=False)),
        axis=1
    )

    datagrouped[["UnitCostJLicenceReel","UnitCostJLicenceTheo","UnitCostJLicenceTheoNego"]]=datagrouped.apply(
        lambda row : pd.Series (get_couts_license_sql_database(datagrouped,row)),
        axis=1
    )
    
    datagrouped[["UsageTotLicence","CostJLicenceReel","CostJLicenceTheo","CostJLicenceTheoNego"]]=datagrouped.apply(
        lambda row : pd.Series (populate_costJLicence_sql_database(row)),
        axis=1
    )

    # datagrouped["CostJLicenceReel"]=0.0
    # datagrouped["CostJLicenceTheo"]=0.0
    # datagrouped["CostJLicenceTheoNego"]=0.0


    datagrouped[["CostJStockage","FacteurRabaisStockage"]]=datagrouped.apply(
        lambda row: pd.Series (populate_costJStockage_sql_database(datagrouped,row)),
        axis=1
    )
    # datagrouped["CostJStockage"]=0.0

    # return datagrouped
    
    datagrouped=datagrouped.fillna(0.0)

    print("Pré traitement 2 terminé")

    
    datagrouped["UnitCostOnDemandNegocie"]=datagrouped["UnitCostOnDemand"]*datagrouped["FacteurRabaisNegociationContrat"]
    
    # datagrouped=dataTest.sort_values(["ResourceName","ResourcePath","Date"])
    
    
    # datagrouped["CostUnitaire"]=datagrouped["CostTot"]/datagrouped["Instances"]
    
    datagrouped["UnitCostReservation1Y"]=datagrouped["UnitCostReservation1Y"].fillna(0)
    # return datagrouped 

    datagrouped["CostJ"]=datagrouped["CostTot"]/datagrouped["NombreJoursMois"]
    datagrouped["CostJI"]=datagrouped["CostJ"]/datagrouped["Instances"]

    # return datagrouped

    datagrouped["UsageJ"]=datagrouped["UsageTot"]/datagrouped["NombreJoursMois"]
    # datagrouped["VariationJI"]=0
    datagrouped["VariationJ"]=0.
    datagrouped["VariationJI"]=0.
    datagrouped["VariationTot"]=0.
    
    datagrouped["GainFonctionalitesSkuJI"]=0.
    datagrouped["GainChangementSkuJI"]=0.
    datagrouped["GainReservation1YJI"]=0.
    datagrouped["GainContratsJI"]=0.
    datagrouped["GainDevTestJI"]=0.

    datagrouped["GainTotDecom"]=0.
    datagrouped["GainTotSku"]=0.
    datagrouped["GainTotSizing"]=0.
    datagrouped["GainTotReservation"]=0.
    datagrouped["GainTotContrats"]=0.
    datagrouped["GainTotDevTest"]=0.
    datagrouped["GainTotOptims"]=0.
    # datagrouped["GainTotDecom"]=0.


    # datagrouped["GainTarifJ"]=0.
    
    datagrouped["GainSizingJ"]=0.
    datagrouped["GainJ"]=0.
    datagrouped["GainDecommissionnementJ"]=0.
    datagrouped["GainJI"]=0.
    datagrouped["GainTot"]=0.
    
    datagrouped= datagrouped.sort_values(["ResourceName","ResourcePath","ResourceGroupName","Config","Date"])
    # datagrouped["CostJLicenceReel"]=datagrouped["NombreJoursMois"]*datagrouped["CostJLicenceTheo"]
    # datagrouped["CostTheoLicence"]=datagrouped["NombreJoursMois"]*datagrouped["CostJLicenceTheo"]
    # datagrouped["CostTheoLicenceNego"]=datagrouped["NombreJoursMois"]*datagrouped["CostJLicenceTheoNego"]
    
    # print(datagrouped.keys())
    # datadecom = datagrouped.copy().iloc[0:0]
    # return datagrouped
    
    index_instance=0
    # with warnings.catch_warnings():
    #     warnings.simplefilter(action='ignore', category=pd.core.common.SettingWithCopyWarning)
        
    while index_instance<len(datagrouped)-1:
        row_reference=datagrouped.iloc[index_instance].copy()
        gainJ_reference=0
        i=index_instance+0

        row_parcourue=datagrouped.iloc[i].copy()
        # if not row_parcourue["ResourceGroupName"]==datagrouped.loc[i,"ResourceGroupName"]:
        #     print(i,row_parcourue["Date"],row_parcourue["ResourceGroupName"],row_parcourue["ResourceName"],datagrouped.loc[i,"ResourceGroupName"])
            
        while i<len(datagrouped) and  row_reference["Date"]==date_min and row_parcourue["ResourcePath"]==row_reference["ResourcePath"] and row_parcourue["ResourceGroupName"]==row_reference["ResourceGroupName"]:

            if i ==index_instance:
                print("Avancement","{:.2%}".format(i/len(datagrouped))," %", i,"/", len(datagrouped))


            
            datagrouped["CostJLicenceReel"].iloc[i]=datagrouped[(datagrouped["Config"]=="Licence")&(datagrouped["ResourceName"]==row_reference["ResourceName"])&(datagrouped["ResourceGroupName"]==row_reference["ResourceGroupName"])&(datagrouped["ResourcePath"]==row_reference["ResourcePath"])&(datagrouped["Date"]==datagrouped["Date"].iloc[i])]["CostJLicenceReel"].sum()
            datagrouped["CostJLicenceTheo"].iloc[i]=datagrouped[(datagrouped["Config"]=="Licence")&(datagrouped["ResourceName"]==row_reference["ResourceName"])&(datagrouped["ResourceGroupName"]==row_reference["ResourceGroupName"])&(datagrouped["ResourcePath"]==row_reference["ResourcePath"])&(datagrouped["Date"]==datagrouped["Date"].iloc[i])]["CostJLicenceTheo"].sum()
            datagrouped["CostJLicenceTheoNego"].iloc[i]=datagrouped[(datagrouped["Config"]=="Licence")&(datagrouped["ResourceName"]==row_reference["ResourceName"])&(datagrouped["ResourceGroupName"]==row_reference["ResourceGroupName"])&(datagrouped["ResourcePath"]==row_reference["ResourcePath"])&(datagrouped["Date"]==datagrouped["Date"].iloc[i])]["CostJLicenceTheoNego"].sum()
            
            datagrouped["CostJStockage"].iloc[i]=datagrouped[(datagrouped["Config"]=="Storage")&(datagrouped["ResourceName"]==row_reference["ResourceName"])&(datagrouped["ResourceGroupName"]==row_reference["ResourceGroupName"])&(datagrouped["ResourcePath"]==row_reference["ResourcePath"])&(datagrouped["Date"]==datagrouped["Date"].iloc[i])]["CostJStockage"].sum()
            datagrouped["FacteurRabaisStockage"].iloc[i]=datagrouped[(datagrouped["Config"]=="Storage")&(datagrouped["ResourceName"]==row_reference["ResourceName"])&(datagrouped["ResourceGroupName"]==row_reference["ResourceGroupName"])&(datagrouped["ResourcePath"]==row_reference["ResourcePath"])&(datagrouped["Date"]==datagrouped["Date"].iloc[i])]["FacteurRabaisStockage"].sum()

            row_parcourue=datagrouped.iloc[i].copy()
            row_reference=datagrouped.iloc[index_instance].copy()

            if row_parcourue["UsageTot"]>0 and row_parcourue["ResourceName"]==row_reference["ResourceName"] and row_parcourue["ResourceGroupName"]==row_reference["ResourceGroupName"] and row_parcourue["Config"]=="Compute":
              
                #Gain fonctionalités
                if row_reference["UnitCostOnDemand"]>0  and row_parcourue["UnitCostOnDemand"]>0 and sorted(row_reference["ResourceSku"])==sorted(row_parcourue["ResourceSku"]):
                    datagrouped["GainFonctionalitesSkuJI"].iloc[i]=(row_parcourue["UnitCostOnDemand"]-row_reference["UnitCostOnDemand"]) *24+row_parcourue["CostJLicenceReel"]-row_reference["CostJLicenceReel"]+(row_parcourue["CostJStockage"]-row_reference["CostJStockage"])
                    # datagrouped["GainFonctionalitesSkuJI"].iloc[i]=0
                
                #Gain changement SKU
                if row_reference["UnitCostOnDemand"]>0  and sorted(row_reference["ResourceSku"])!=sorted(row_parcourue["ResourceSku"]): #pas changement nb instances, pas changement sku, pas réservation

                    datagrouped["GainChangementSkuJI"].iloc[i]=24*(row_parcourue["UnitCostOnDemand"].copy()-row_reference["UnitCostOnDemand"].copy()) + row_parcourue["CostJLicenceTheo"]-row_reference["CostJLicenceTheo"] + (row_parcourue["CostJStockage"]-row_reference["CostJStockage"])
                    # print(row_parcourue["CostJLicenceTheo"]-row_reference["CostJLicenceTheo"],row_parcourue["CostJStockage"]-row_reference["CostJStockage"])

                #Gain Sizing
                if row_reference["UnitCostOnDemand"]>0 : #pas changement nb instances, pas changement sku, pas contrat
                    # datagrouped["GainSizingJ"].iloc[i]=24*(row_parcourue["UsageTot"]/(24*row_parcourue["NombreJoursMois"])-row_reference["UsageTot"]/(24*row_reference["NombreJoursMois"]))*(row_parcourue["CostTot"]/row_parcourue["UsageTot"]  )+24*(row_parcourue["UsageTotLicence"]/(24*row_parcourue["NombreJoursMois"])-row_reference["UsageTotLicence"]/(24*row_reference["NombreJoursMois"]))*row_parcourue["CostJLicenceTheo"]/max(1,row_parcourue["UsageTotLicence"])
                    datagrouped["GainSizingJ"].iloc[i]=24*(row_parcourue["UsageTot"]/(24*row_parcourue["NombreJoursMois"])-row_reference["UsageTot"]/(24*row_reference["NombreJoursMois"]))*(row_parcourue["CostTot"]/row_parcourue["UsageTot"]) +24*(row_parcourue["UsageTotLicence"]/(24*row_parcourue["NombreJoursMois"])-row_reference["UsageTotLicence"]/(24*row_reference["NombreJoursMois"]))*row_parcourue["CostJLicenceTheo"]/max(1,row_parcourue["UsageTotLicence"])


                #Gain Reservation
                if row_parcourue["UsageReservation1Y"]>0: #pas changement nb instances, pas changement sku, pas contrat
                    # print(row_parcourue["UnitCostReservation1Y"],row_reference["UnitCostOnDemand"],row_parcourue["UsageReservation1Y"],row_parcourue["Instances"]   )
                    # row_precedente=datagrouped.iloc[i-1]
                    datagrouped["GainReservation1YJI"].iloc[i]=24*(row_parcourue["UnitCostReservation1Y"]-row_parcourue["UnitCostOnDemandNegocie"])*row_parcourue["UsageReservation1Y"]/row_parcourue["UsageTot"]
                
                #Gain Contrats
                if gain_contrat:
                    datagrouped["GainContratsJI"].iloc[i]=24*(row_parcourue["UnitCostOnDemandNegocie"]-row_parcourue["UnitCostOnDemand"]) +row_parcourue["CostJLicenceTheoNego"]-row_parcourue["CostJLicenceTheo"] -row_parcourue["CostJStockage"]*(1-row_parcourue["FacteurRabaisStockage"])
                
                # print("TEST",row_parcourue["CostJStockage"]*(1-row_parcourue["FacteurRabaisStockage"]))
                #GainDevTest
                if row_parcourue["CostJLicenceTheoNego"]>0 and abs((row_parcourue["CostJLicenceTheoNego"]-row_parcourue["CostJLicenceReel"])/row_parcourue["CostJLicenceTheoNego"])>0.03:
                    datagrouped["GainDevTestJI"].iloc[i]= (row_parcourue["CostJLicenceReel"]-row_parcourue["CostJLicenceTheoNego"])
                # else:

                # print(coutLicenceReelJ,coutLicenceTheoJ,coutLicenceTheoNegoJ)
                 
                datagrouped["CostJ"].iloc[i]=datagrouped["CostJ"].iloc[i] +  row_parcourue["CostJLicenceReel"] + row_parcourue["CostJStockage"]*row_parcourue["FacteurRabaisStockage"]
                datagrouped["CostJI"].iloc[i]=datagrouped["CostJ"].iloc[i]/datagrouped["Instances"].iloc[i]
                # datagrouped["CostOnDemand"].iloc[i]=datagrouped["CostOnDemand"].iloc[i]  +(coutLicenceReelJ+coutStockageJ)*datagrouped["NombreJoursMois"].iloc[i]
                datagrouped["CostTot"].iloc[i]=datagrouped["CostJ"].iloc[i]*datagrouped["NombreJoursMois"].iloc[i]
                if i==index_instance:
                    row_reference=datagrouped.iloc[index_instance].copy()


                # datagrouped["VariationTot"].iloc[i]=row_parcourue["CostTot"]-row_reference["CostTot"]
                # datagrouped["CostJI"].iloc[i]=datagrouped["CostJ"].iloc[i]/datagrouped["Instances"].iloc[i]
                datagrouped["VariationJ"].iloc[i]= datagrouped["CostJ"].iloc[i]-row_reference["CostJ"]
                datagrouped["VariationJI"].iloc[i]= datagrouped["CostJI"].iloc[i]-row_reference["CostJI"]

                datagrouped["VariationTot"].iloc[i]= (datagrouped["CostJ"].iloc[i]-row_reference["CostJ"])*datagrouped["NombreJoursMois"].iloc[i]
                
                datagrouped["GainJI"].iloc[i]=datagrouped["GainChangementSkuJI"].iloc[i]+datagrouped["GainReservation1YJI"].iloc[i]+datagrouped["GainContratsJI"].iloc[i]+datagrouped["GainFonctionalitesSkuJI"].iloc[i]+ datagrouped["GainDevTestJI"].iloc[i]
                
                if i==index_instance and not gains_premier_mois:
                    gainJ_reference=(datagrouped["GainJI"].iloc[i])*row_reference["Instances"]+datagrouped["GainSizingJ"].iloc[i]

                datagrouped["GainJ"].iloc[i]=datagrouped["GainJI"].iloc[i]*1+datagrouped["GainSizingJ"].iloc[i]-gainJ_reference
                # print(gainJ_reference)
                # datagrouped["GainJ"].iloc[i]=(datagrouped["GainReservation1YJI"].iloc[i]+datagrouped["GainChangementSkuJI"].iloc[i]+datagrouped["GainContratsJI"].iloc[i])*row_reference["Instances"]+datagrouped["GainSizingJ"].iloc[i]
                # datagrouped["VariationTot"].iloc[i]=row_parcourue["CostTot"]-row_reference["CostTot"]

                datagrouped["GainTotSku"].iloc[i]=(datagrouped["GainChangementSkuJI"].iloc[i]+datagrouped["GainFonctionalitesSkuJI"].iloc[i])*row_reference["Instances"]*datagrouped["NombreJoursMois"].iloc[i]
                datagrouped["GainTotSizing"].iloc[i]=datagrouped["GainSizingJ"].iloc[i]*datagrouped["NombreJoursMois"].iloc[i]
                datagrouped["GainTotSizingComp"].iloc[i]=datagrouped["GainSizingCompJ"].iloc[i]*datagrouped["NombreJoursMois"].iloc[i]

                datagrouped["GainTotReservation"].iloc[i]=datagrouped["GainReservation1YJI"].iloc[i]*row_reference["Instances"]*datagrouped["NombreJoursMois"].iloc[i]
                datagrouped["GainTotContrats"].iloc[i]=datagrouped["GainContratsJI"].iloc[i]*row_reference["Instances"]*datagrouped["NombreJoursMois"].iloc[i]
                datagrouped["GainTotDevTest"].iloc[i]=datagrouped["GainDevTestJI"].iloc[i]*row_reference["Instances"]*datagrouped["NombreJoursMois"].iloc[i]

                # data_sour datagrouped["GainDevTestJI"].iloc[i]
                #Gain Décommissionnement
                
                if row_parcourue["Date"]<date_max:# and False:
                    if i<len(datagrouped)-1:
                        row_suivante=datagrouped.iloc[i+1].copy()
                        
                        
                    #déclencher le décom
                    
                    if ( i<len(datagrouped)-1 and (row_suivante["ResourceName"]!=row_parcourue["ResourceName"] or row_suivante["ResourceGroupName"]!=row_parcourue["ResourceGroupName"] )) or i==len(datagrouped)-1:
                        
                        # Add one month to the date using relativedelta
                        last_date = datetime.strptime(row_parcourue["Date"], "%Y-%m-%d")
                        row_decom=datagrouped.iloc[i].copy()
                        # print(row_decom["GainSizingJ"])
                        
                        row_decom["UsageTot"]=0.
                        row_decom["CostTot"]=0.
                        row_decom["UsageOnDemand"]=0.
                        row_decom["CostOnDemand"]=0.
                        row_decom["UsageReservation1Y"]=0.
                        row_decom["CostReservation1Y"]=0.
                        row_decom["InstancesBrut"]=0.
                        row_decom["CostJI"]=0.
                        row_decom["UsageJ"]=0.
                        row_decom["Instances"]=0.
                        row_decom["VariationJ"]=-row_reference["CostTot"].copy()/row_reference["NombreJoursMois"].copy()
                        # row_decom["VariationJI"]=-row_reference["CostTotJI"]
                        
                        row_decom["GainDecommissionnementJ"]=-row_parcourue["CostJ"].copy()
                        row_decom["GainJ"]=row_decom["GainJ"]+row_decom["GainDecommissionnementJ"]
                        row_decom["CostJ"]=0.


                        while last_date.strftime("%Y-%m-%d")<date_max: # ajout lignes jusqu'à date max
                            
                            new_date = (last_date + relativedelta(months=1))
                            
                            row_decom["Date"]=new_date.strftime("%Y-%m-%d")
                            row_decom["NombreJoursMois"]=get_count_days(new_date.strftime("%Y-%m-%d"))

                            row_decom["VariationTot"]=-row_reference["CostTot"].copy()/row_reference["NombreJoursMois"]*row_decom["NombreJoursMois"]
                            row_decom["GainTot"]=row_decom["GainJ"]*row_decom["NombreJoursMois"]



                            row_decom["GainTotSku"]=(row_decom["GainChangementSkuJI"]+row_decom["GainFonctionalitesSkuJI"])*row_reference["Instances"]*row_decom["NombreJoursMois"]
                            row_decom["GainTotSizing"]=row_decom["GainSizingJ"]*row_decom["NombreJoursMois"]
                            row_decom["GainTotSizingComp"]=row_decom["GainSizingCompJ"]*row_decom["NombreJoursMois"]

                            row_decom["GainTotReservation"]=row_decom["GainReservation1YJI"]*row_reference["Instances"]*row_decom["NombreJoursMois"]
                            row_decom["GainTotContrats"]=row_decom["GainContratsJI"]*row_reference["Instances"]*row_decom["NombreJoursMois"]
                            row_decom["GainTotDevTest"]=row_decom["GainDevTestJI"]*row_reference["Instances"]*row_decom["NombreJoursMois"]

                            row_decom["GainTotDecom"]=row_decom["GainDecommissionnementJ"]*row_decom["NombreJoursMois"]
                                                                
                            last_date = new_date

                            datagrouped = pd.concat([datagrouped, pd.DataFrame([row_decom]).reset_index(drop=True)], ignore_index=True, axis=0).reset_index(drop=True)

                            i+=1
                        datagrouped=datagrouped.sort_values(["ResourceName","ResourcePath","ResourceGroupName","Config","Date"])

                row_parcourue=datagrouped.iloc[i].copy()
            i+=1
            
        index_instance+=1

    
    datagrouped["GainTot"]= datagrouped["GainJ"]*datagrouped["NombreJoursMois"]
    datagrouped["GainTotOptims"]=datagrouped["GainTotDecom"]+datagrouped["GainTotSku"]+datagrouped["GainTotContrats"]+datagrouped["GainTotSizing"]+datagrouped["GainTotReservation"]+datagrouped["GainTotDevTest"]
    datagrouped["Ecart"]=datagrouped["VariationTot"]-datagrouped["GainTot"]
    # datagrouped["GainJI"]=datagrouped["GainChangementSkuJI"]+datagrouped["GainReservation1YJI"]+datagrouped["GainContratsJI"]

    datagrouped=datagrouped[["Date","NombreJoursMois","ProjectName","ApplicationName","LocationName","ServiceName","ResourcePath","ResourceGroupName","ResourceName","Instances","Config","ResourceSku","UsageTot","CostTot","UsageOnDemand","CostOnDemand","UsageReservation1Y","CostReservation1Y","UnitCostOnDemand","UnitCostOnDemandNegocie","UnitCostR1Y","UnitCostJLicenceReel","CostJLicenceReel","UnitCostJLicenceTheo","CostJLicenceTheo","UnitCostJLicenceTheoNego","CostJLicenceTheoNego","CostJStockage","CostJ","VariationJ","GainJ","CostJI","VariationJI","GainJI","GainFonctionalitesSkuJI","GainChangementSkuJI","GainReservation1YJI","GainContratsJI","GainDevTestJI","GainSizingJ","GainDecommissionnementJ","VariationTot","GainTot","Ecart"]]

    return datagrouped[datagrouped["Config"]=="Compute"]


     
donnees=resourcesCoreModel.copy()
# donnees=donnees[donnees["ProjectName"]=="IT-PROD-US"]
date_min="2022-10-01"
date_max="2023-12-01"
resourceName="sqldb-it-cmss-prod01-cmss_emailfunctionalityenability"
# resourceName="sqldb_alm_wsa"
test=get_gains_totaux_sql_database(donnees,date_min,date_max,gains_premier_mois=False,resourceName=resourceName)

test.to_excel("C:\\Users\\hugo.dufrene\\Wavestone\\WE - ROLEX [CH] - Support mise en place Gouvernance Cloud public - 13665 - Documents partages\\Gouvernance Cloud\\FinOps\\Optimisations\\SQL Database\\20240131 - Analyse Gains Optims SQL.xlsx",index=False)
# test[test["ResourceGroupName"]=="rg-xd-cmss-sfcm"]
test
 


# %%
def get_gains_dimensionnement_app_service(donnees,date_min):
    donnees=donnees[donnees["ServiceName"]=="Azure App Service"]
    # donnees=donnees[donnees["ResourceName"]=="asp-cn-cmss-prod01"]
    data_source = donnees.groupby(["Date","ProjectName","ServiceName","ResourceName"])[["CostTot","UsageTot","CostOnDemand","UsageOnDemand"]].sum().reset_index()
    data_source["UnitCostOnDemand"]=0.0
    data_source["UnitCostOnDemandNegocie"]=0.0
    
    data=donnees.groupby(["Date","ResourceName","ProjectName","ApplicationName","LocationName","ServiceName","ResourceGroupName","ResourcePath","ServiceTier","ResourceSkuSize"])[["UsageTot","UsageOnDemand","CostTot","CostOnDemand"]].sum().reset_index()
   
    data=data[data["Date"]>=date_min]
    
    data= data.sort_values(["ResourceName","ResourcePath","Date"])
  
    gains=None
 
    data = data.fillna(0)
    data["Config"] = data.apply(lambda row: get_config(row), axis=1)
    
    datagrouped=data.groupby(["Date","ResourceName","ProjectName","ApplicationName","LocationName","ServiceName","ResourceGroupName","ResourcePath","Config"])[["UsageTot","CostTot","UsageOnDemand","CostOnDemand"]].sum().reset_index()
 
    datagrouped["UnitCostOnDemand"]=0
    
    datagrouped[["UnitCostOnDemand", "FacteurRabaisNegociationContrat"]] = datagrouped.apply(
        lambda row: pd.Series(get_unit_cost_on_demand_v2(data_source, row, "Azure App Service", negocie=False)),
        axis=1
    )
    
    # datagrouped=dataTest.sort_values(["ResourceName","ResourcePath","Date"])
    datagrouped["InstancesSizing"]=datagrouped.apply(lambda row:get_nb_instances("Azure App Service,row["Date"],row["UsageTot"]),axis=1)
    datagrouped["CostUnitaire"]=datagrouped["CostTot"]/datagrouped["InstancesSizing"]

    datagrouped["GainTot"]=0
    datagrouped=datagrouped.sort_values(["ResourceName","ResourcePath","Date"])
    i=1
    
    j=0
    while i<len(datagrouped):
        
        
        if datagrouped.iloc[i-1]["ResourceName"]==datagrouped.iloc[i]["ResourceName"] and datagrouped.iloc[i-1]["InstancesSizing"]!=0 and datagrouped.iloc[i]["InstancesSizing"]!=0 and datagrouped.iloc[i-1]["InstancesSizing"]==round(datagrouped.iloc[i-1]["InstancesSizing"]) and datagrouped.iloc[i-1]["InstancesSizing"]!=datagrouped.iloc[i]["InstancesSizing"]:
            
            # print()
            
            j=0
            while i+j<len(datagrouped) and datagrouped.iloc[i+j]["ResourceName"]==datagrouped.iloc[i-1]["ResourceName"] and  datagrouped.iloc[i+j]["GainTot"]==0:
                
                
                if gains_decommissionnement[(gains_decommissionnement["Date"]==datagrouped.iloc[i+j]["Date"])&(gains_decommissionnement["ResourceName"]==datagrouped.iloc[i+j]["ResourceName"])].empty and datagrouped.iloc[i+j]["InstancesSizing"]>0 and datagrouped.iloc[i+j]["CostUnitaire"]>0:
                    
                    # if datagrouped.iloc[i+j]["ResourceName"]=="asp-cn-cmss-prod01":
                    #     print()
                    #     print("REFERENCE",datagrouped.iloc[i-1]["Date"],datagrouped.iloc[i-1]["ResourceName"],datagrouped.iloc[i-1]["InstancesSizing"])
                    #     print("GAIN",datagrouped.iloc[i+j]["Date"],datagrouped.iloc[i+j]["ResourceName"],datagrouped.iloc[i+j]["InstancesSizing"],(datagrouped.iloc[i+j]["InstancesSizing"]-datagrouped.iloc[i-1]["InstancesSizing"])*datagrouped.iloc[i+j]["CostUnitaire"])
                    #     # print(i+j)
                    datagrouped["GainTot"].iloc[i+j]=(datagrouped["InstancesSizing"].iloc[i+j]-datagrouped["InstancesSizing"].iloc[i-1])*datagrouped["CostUnitaire"].iloc[i+j]
                    # print(i+j,datagrouped["GainSizing"].iloc[i+j])
                    # print("GAIN",datagrouped.iloc[i+j]["Date"],datagrouped.iloc[i+j]["ResourceName"],datagrouped.iloc[i+j]["InstancesSizing"],(datagrouped.iloc[i+j]["InstancesSizing"]-datagrouped.iloc[i-1]["InstancesSizing"])*datagrouped.iloc[i+j]["CostUnitaire"])
                    
                
                # print(i,j,i+j,":",397,"=>",datagrouped.loc[397, 'GainSizing'])
                j+=1
        i+=1
    

    # print(397,":",datagrouped.iloc[397]["Date"],datagrouped.iloc[397]["ResourceName"],"=>",datagrouped.iloc[397]['GainSizing'])
    return datagrouped

    
gains_sizing_app_service = get_gains_dimensionnement_app_service(donnees,date_min)
%store gains_sizing_app_service
# dataTest
# 

# %%
def get_couts_license_sql_database(datagrouped,row_parcourue):
    dfCoutLicence= datagrouped[(datagrouped["Config"]=="Licence")&(datagrouped["ResourceName"]==row_parcourue["ResourceName"])&(datagrouped["ResourceGroupName"]==row_parcourue["ResourceGroupName"])&(datagrouped["ResourcePath"]==row_parcourue["ResourcePath"])&(datagrouped["Date"]==row_parcourue["Date"])]
    rabais = 0.11

    if not dfCoutLicence.empty:
        sku = dfCoutLicence["ResourceSku"].iloc[0][0]
        sku=sku.split("-")[0]+"- SQL License"

        if  azure_prices[(row_parcourue["ProjectId"]==azure_prices["ProjectId"])].empty:
            
            if dfCoutLicence["CostTot"].iloc[0]>0 and dfCoutLicence["UsageTot"].iloc[0]>0:

                if row_parcourue["Date"]>="2023-09-01":
                    return(dfCoutLicence["CostTot"].iloc[0]/dfCoutLicence["UsageTot"].iloc[0],dfCoutLicence["CostTot"].iloc[0]/dfCoutLicence["UsageTot"].iloc[0]/(1-rabais),dfCoutLicence["CostTot"].iloc[0]/dfCoutLicence["UsageTot"].iloc[0])

                else:
                    return(dfCoutLicence["CostTot"].iloc[0]/dfCoutLicence["UsageTot"].iloc[0],dfCoutLicence["CostTot"].iloc[0]/dfCoutLicence["UsageTot"].iloc[0],dfCoutLicence["CostTot"].iloc[0]/dfCoutLicence["UsageTot"].iloc[0])


        
        else:
            if row_parcourue["Date"]>="2023-09-01":        
                coutLicenceNonNegoJI = azure_prices[(row_parcourue["ProjectId"]==azure_prices["ProjectId"])&(~azure_prices["meterSubCategory"].isna())&(azure_prices["meterName"]=="vCore")&(azure_prices["meterSubCategory"]==sku)&(azure_prices["unitPrice"]>0)&(azure_prices["Date"]=="2023-08-01")].sort_values("Date",ascending=False)["unitPrice"].iloc[0]
                coutLicenceNegoJI = azure_prices[(row_parcourue["ProjectId"]==azure_prices["ProjectId"])&(~azure_prices["meterSubCategory"].isna())&(azure_prices["meterName"]=="vCore")&(azure_prices["meterSubCategory"]==sku)&(azure_prices["Date"]==row_parcourue["Date"])&(azure_prices["unitPrice"]>0)].sort_values("Date")
                if coutLicenceNegoJI.empty:
                    coutLicenceNegoJI=coutLicenceNonNegoJI
                else :
                    coutLicenceNegoJI=coutLicenceNegoJI["unitPrice"].iloc[0]
                # print(coutLicenceNegoJI)
                return(dfCoutLicence["CostTot"].iloc[0]/dfCoutLicence["UsageTot"].iloc[0],coutLicenceNonNegoJI/100*row_parcourue["vCore"],coutLicenceNegoJI/100*row_parcourue["vCore"])
            
            else:
                coutLicenceNegoJI = azure_prices[(row_parcourue["ProjectId"]==azure_prices["ProjectId"])&(~azure_prices["meterSubCategory"].isna())&(azure_prices["meterName"]=="vCore")&(azure_prices["meterSubCategory"]==sku)&(azure_prices["Date"]==row_parcourue["Date"])&(azure_prices["unitPrice"]>0)].sort_values("Date")
                if coutLicenceNegoJI.empty:
                    coutLicenceNegoJI=0.0
                else :
                    coutLicenceNegoJI=coutLicenceNegoJI["unitPrice"].iloc[0]
            
                return(dfCoutLicence["CostTot"].iloc[0]/dfCoutLicence["UsageTot"].iloc[0],coutLicenceNegoJI/100*row_parcourue["vCore"],coutLicenceNegoJI/100*row_parcourue["vCore"])


    # return  dfCoutLicence["CostTot"].sum()/row_parcourue["NombreJoursMois"]
    return (0.0,0.0,0.0)

def populate_costJLicence(ligne_licence,service):
    if ligne_licence["Config"]=="Licence":
        return (
                ligne_licence["UsageTot"],
                ligne_licence["CostTot"]/ligne_licence["NombreJoursMois"],
                ligne_licence["UsageTot"]*ligne_licence["UnitCostJLicenceTheo"]/ligne_licence["NombreJoursMois"],
                ligne_licence["UsageTot"]*ligne_licence["UnitCostJLicenceTheoNego"]/ligne_licence["NombreJoursMois"])

    else:
        
        return (0.0,0.0,0.,0.)
    

def populate_costJStockage(datagrouped,ligne):
    coutStockageJ=0.0
    facteur=0.11
    if ligne["Config"]=="Storage":
        dfCoutStockage=datagrouped[(datagrouped["Config"]=="Storage")&(datagrouped["ResourceName"]==ligne["ResourceName"])&(datagrouped["ResourceGroupName"]==ligne["ResourceGroupName"])&(datagrouped["ResourcePath"]==ligne["ResourcePath"])&(datagrouped["Date"]==ligne["Date"])]

        if not dfCoutStockage.empty:
            coutStockageJ=dfCoutStockage["CostTot"].sum()/ligne["NombreJoursMois"]          

    if ligne["Date"]>="2023-09-01":
        return (coutStockageJ/(1-facteur),(1-facteur))
    
    else:
        return (coutStockageJ,1.0)





# %%
def get_gains_totaux_app_service_legacy(donnees,date_min,date_max,gain_contrat=True,gains_premier_mois=True,resourceName=None):
    # date_max=sorted(list(donnees["Date"].unique()))[-1]

    donnees=donnees[donnees["ServiceName"]=="Azure App Service"]
    
    if not resourceName==None:
        donnees=donnees[(donnees["ResourceName"]==resourceName)]
    # gains_tot_sql_database_CoreModel[gains_tot_sql_database_CoreModel["ResourceName"]=="xd-cmss-dev"]
    # donnees=donnees[(donnees["ProjectName"]=='IT-PROD-IT')]
    
    data=donnees.groupby(["Date","ResourceName","ProjectName","ApplicationName","LocationName","ServiceName","ResourceGroupName","ResourcePath","ServiceTier","ResourceSkuTier","ResourceSkuSize"])[["UsageTot","UsageOnDemand","CostTot","CostOnDemand","CostReservation1Y","UsageReservation1Y"]].sum().reset_index()    

    
    # return data
    gains=None
 
    data = data.fillna(0)
    # return data
    data["Config"] = data.apply(lambda row: get_config(row), axis=1)
    # data["Capacité"]=data.apply(lambda row:get_nb_vcore(row),axis=1)
    # return data
    data["ResourceSku"]=data.apply(lambda row:row["ResourceSkuTier"]+" "+row["ResourceSkuSize"],axis=1)
    
    # data["UsageTot"]=data.apply(lambda row:row["UsageTot"]*24 if "DTU" in "ResourceSkuSize" else row["UsageTot"],axis=1)
    
    # data["UsageOnDemand"]=data.apply(lambda row:row["UsageOnDemand"]*24 if "DTU" in row["ResourceSkuSize"] else row["UsageOnDemand"],axis=1)
    data["InstancesBrut"]=data.apply(lambda row:get_nb_instances("Azure App Service",row,brut=True),axis=1)
    data["Instances"]=data.apply(lambda row:get_nb_instances("Azure App Service",row,brut=False),axis=1)

    # data["UsageTotBrut"] = data.apply(lambda row: row["UsageTot"] * 24 if "DTU" in row["ResourceSkuSize"] else row["UsageTot"], axis=1)
    # data["UsageTot"] = data.apply(lambda row: row["UsageTot"] * 24 if "DTU" in row["ResourceSkuSize"] else row["UsageTot"]/max(row["Capacité"],1), axis=1)
    data["UsageReservation1YBrut"]=data["UsageReservation1Y"].copy()
    data["UsageOnDemandBrut"]=data["UsageOnDemand"].copy()

    # data["UsageOnDemand"] = data.apply(lambda row:row["UsageOnDemand"] * 24 if "DTU" in row["ResourceSkuSize"] else row["UsageOnDemand"]/max(row["Capacité"],1),axis=1)
    # data["UsageReservation1Y"] = data.apply(lambda row: row["UsageReservation1Y"] * 24 if "DTU" in row["ResourceSkuSize"] else row["UsageReservation1Y"]/max(row["Capacité"],1), axis=1)

    # return data

    data_source=data.copy()
    # data_source = donnees.groupby(["Date","ProjectName","ServiceName","ResourceName"])[["CostTot","UsageTot","CostOnDemand","UsageOnDemand","CostReservation1Y","UsageReservation1Y"]].sum().reset_index()
    data_source["UnitCostOnDemand"]=0.0
    data_source["UnitCostOnDemandNegocie"]=0.0
    
    data=data[data["Date"]>=date_min]
    
    # return data

#     data_source["UsageTot"] =data_source.apply(lambda row:row["UsageTot"] * 24 if "DTU" in row["ResourceSkuSize"] else row["UsageTot"]/max(row["Capacité"],1),axis=1)
# data_source["UsageOnDemand"] =data_source.apply(lambda row:row["UsageOnDemand"] * 24 if "DTU" in row["ResourceSkuSize"] else row["UsageOnDemand"]/max(row["Capacité"],1),axis=1)
    # return data
    
    datagrouped=data.groupby(["Date","ResourceName","ProjectName","ApplicationName","LocationName","ServiceName","ResourceGroupName","ResourcePath","Config"])[["UsageTot","CostTot","UsageOnDemand","UsageOnDemandBrut","CostOnDemand","CostReservation1Y","UsageReservation1Y","UsageReservation1YBrut","Instances","InstancesBrut"]].sum().reset_index()

    datagrouped["ResourceSku"] = datagrouped.apply(lambda row: list(data[
        (data["Date"] == row["Date"]) &
        (data["ResourceName"] == row["ResourceName"])&(data["Config"]==row["Config"])    ]["ResourceSku"].unique()), axis=1) 
    
    datagrouped["UnitCostOnDemand"]=0.
    datagrouped["UnitCostOnDemandNegocie"]=0.
    datagrouped["UnitCostR1Y"]=0.
    # return data_source
    
    print("Pré traitement 1 terminé")
    # return datagrouped

    datagrouped[["UnitCostOnDemand", "FacteurRabaisNegociationContrat"]] = datagrouped.apply(
        lambda row: pd.Series(get_unit_cost_on_demand_v2(data_source, row, "Azure App Service", negocie=False)),
        axis=1
    )
    
    print("Pré traitement 2 terminé")

    
    datagrouped["UnitCostOnDemandNegocie"]=datagrouped["UnitCostOnDemand"]*datagrouped["FacteurRabaisNegociationContrat"]
    
    # datagrouped=dataTest.sort_values(["ResourceName","ResourcePath","Date"])
    datagrouped["NombreJoursMois"]=datagrouped["Date"].apply(lambda row:get_count_days(row))
    
    
    # datagrouped["CostUnitaire"]=datagrouped["CostTot"]/datagrouped["Instances"]
    datagrouped["UnitCostReservation1Y"]=datagrouped["CostReservation1Y"]/datagrouped["UsageReservation1Y"]
    datagrouped["UnitCostReservation1Y"]=datagrouped["UnitCostReservation1Y"].fillna(0)
    # return datagrouped

    datagrouped["CostJ"]=datagrouped["CostTot"]/datagrouped["NombreJoursMois"]
    datagrouped["CostJI"]=datagrouped["CostJ"]/datagrouped["Instances"]

    # return datagrouped

    datagrouped["UsageJ"]=datagrouped["UsageTot"]/datagrouped["NombreJoursMois"]
    # datagrouped["VariationJI"]=0
    datagrouped["VariationJ"]=0.
    datagrouped["VariationJI"]=0.
    datagrouped["VariationTot"]=0.
    
    datagrouped["GainFonctionalitesSkuJI"]=0.
    datagrouped["GainChangementSkuJI"]=0.
    datagrouped["GainReservation1YJI"]=0.
    datagrouped["GainContratsJI"]=0.
    datagrouped["GainDevTestJI"]=0.

    datagrouped["GainTotDecom"]=0.
    datagrouped["GainTotSku"]=0.
    datagrouped["GainTotSizing"]=0.
    datagrouped["GainTotReservation"]=0.
    datagrouped["GainTotContrats"]=0.
    datagrouped["GainTotOptims"]=0.
    # datagrouped["GainTotDecom"]=0.


    # datagrouped["GainTarifJ"]=0.
    
    datagrouped["GainSizingJ"]=0.
    datagrouped["GainJ"]=0.
    datagrouped["GainDecommissionnementJ"]=0.
    datagrouped["GainJI"]=0.
    datagrouped["GainTot"]=0.
    
    datagrouped= datagrouped.sort_values(["ResourceName","ResourcePath","ResourceGroupName","Config","Date"])
    # datadecom = datagrouped.copy().iloc[0:0]
    # return datagrouped
    
    index_instance=0
    # with warnings.catch_warnings():
    #     warnings.simplefilter(action='ignore', category=pd.core.common.SettingWithCopyWarning)
        
    while index_instance<len(datagrouped)-1:
        row_reference=datagrouped.iloc[index_instance]
        gainJ_reference=0
        i=index_instance+0

        row_parcourue=datagrouped.iloc[i].copy()
          
        date_min=datagrouped[(datagrouped["ResourcePath"]==row_reference["ResourcePath"])&(datagrouped["ResourceGroupName"]==row_reference["ResourceGroupName"])].sort_values("Date")["Date"].iloc[0]


        while i<len(datagrouped) and row_reference["Date"]==date_min and row_parcourue["ResourcePath"]==row_reference["ResourcePath"] and row_parcourue["ResourceGroupName"]==row_reference["ResourceGroupName"]:
            row_parcourue=datagrouped.iloc[i].copy()
                        
            print(i,row_parcourue["Date"],row_parcourue["ResourceGroupName"],row_parcourue["ResourceName"])

            if row_parcourue["UsageTot"]>0 and row_parcourue["ResourceName"]==row_reference["ResourceName"] and row_parcourue["ResourceGroupName"]==row_reference["ResourceGroupName"]:
                
                #Gain fonctionalités
                if row_reference["UnitCostOnDemand"]>0  and row_parcourue["UnitCostOnDemand"]>0 and sorted(row_reference["ResourceSku"])==sorted(row_parcourue["ResourceSku"]):
                    datagrouped["GainFonctionalitesSkuJI"].iloc[i]=24*(row_parcourue["UnitCostOnDemand"]-row_reference["UnitCostOnDemand"]) #*row_parcourue["UsageTot"]/(row_parcourue["NombreJoursMois"])

                
                #Gain changement SKU
                if row_reference["UnitCostOnDemand"]>0  and sorted(row_reference["ResourceSku"])!=sorted(row_parcourue["ResourceSku"]): #pas changement nb instances, pas changement sku, pas réservation
                    val1 = float(row_parcourue["UnitCostOnDemand"].copy())
                    val2=float(row_reference["UnitCostOnDemand"].copy())
                    datagrouped["GainChangementSkuJI"].iloc[i]=24*(val1-val2)
                    # print(i,24*(row_parcourue["UnitCostOnDemand"]-row_reference["UnitCostOnDemand"]),datagrouped.at[i,"GainChangementSkuJI"])
                #Gain Sizing
                if row_reference["UnitCostOnDemand"]>0 : #pas changement nb instances, pas changement sku, pas contrat
                    datagrouped["GainSizingJ"].iloc[i]=24*(row_parcourue["Instances"]-row_reference["Instances"])*row_parcourue["CostTot"]/row_parcourue["UsageTot"]
                    
                #Gain Reservation
                if row_parcourue["UsageReservation1Y"]>0: #pas changement nb instances, pas changement sku, pas contrat
                    # print(row_parcourue["UnitCostReservation1Y"],row_reference["UnitCostOnDemand"],row_parcourue["UsageReservation1Y"],row_parcourue["Instances"]   )
                    # row_precedente=datagrouped.iloc[i-1]
                    datagrouped["GainReservation1YJI"].iloc[i]=24*(row_parcourue["UnitCostReservation1Y"]-row_parcourue["UnitCostOnDemandNegocie"])*row_parcourue["UsageReservation1Y"]/row_parcourue["UsageTot"]
                
                #Gain Contrats
                if row_parcourue["UnitCostOnDemand"]!=row_parcourue["UnitCostOnDemandNegocie"] and gain_contrat:
                    datagrouped["GainContratsJI"].iloc[i]=24*(row_parcourue["UnitCostOnDemandNegocie"]-row_parcourue["UnitCostOnDemand"])  # *row_parcourue["UsageOnDemand"]/row_parcourue["UsageTot"]
                
                # datagrouped["VariationTot"].iloc[i]=row_parcourue["CostTot"]-row_reference["CostTot"]
                datagrouped["CostJI"].iloc[i]=datagrouped["CostJ"].iloc[i]/datagrouped["Instances"].iloc[i]
                datagrouped["VariationJ"].iloc[i]= datagrouped["CostJ"].iloc[i]-row_reference["CostJ"]
                datagrouped["VariationJI"].iloc[i]= datagrouped["CostJI"].iloc[i]-row_reference["CostJI"]

                datagrouped["VariationTot"].iloc[i]= (datagrouped["CostJ"].iloc[i]-row_reference["CostJ"])*datagrouped["NombreJoursMois"].iloc[i]
                
                datagrouped["GainJI"].iloc[i]=datagrouped["GainChangementSkuJI"].iloc[i]+datagrouped["GainReservation1YJI"].iloc[i]+datagrouped["GainContratsJI"].iloc[i]+datagrouped["GainFonctionalitesSkuJI"].iloc[i]
                
                if i==index_instance and not gains_premier_mois:
                    gainJ_reference=(datagrouped["GainJI"].iloc[i])*row_reference["Instances"]+datagrouped["GainSizingJ"].iloc[i]

                datagrouped["GainJ"].iloc[i]=datagrouped["GainJI"].iloc[i]*row_reference["Instances"]+datagrouped["GainSizingJ"].iloc[i]-gainJ_reference
                # print(gainJ_reference)
                # datagrouped["GainJ"].iloc[i]=(datagrouped["GainReservation1YJI"].iloc[i]+datagrouped["GainChangementSkuJI"].iloc[i]+datagrouped["GainContratsJI"].iloc[i])*row_reference["Instances"]+datagrouped["GainSizingJ"].iloc[i]
                datagrouped["CostJI"].iloc[i]=datagrouped["CostJ"].iloc[i]/datagrouped["Instances"].iloc[i]
                # datagrouped["VariationTot"].iloc[i]=row_parcourue["CostTot"]-row_reference["CostTot"]
                
                
                
                datagrouped["GainTotSku"].iloc[i]=(datagrouped["GainChangementSkuJI"].iloc[i]+datagrouped["GainFonctionalitesSkuJI"].iloc[i])*row_reference["Instances"]*datagrouped["NombreJoursMois"].iloc[i]
                datagrouped["GainTotSizing"].iloc[i]=datagrouped["GainSizingJ"].iloc[i]*datagrouped["NombreJoursMois"].iloc[i]
                datagrouped["GainTotReservation"].iloc[i]=datagrouped["GainReservation1YJI"].iloc[i]*row_reference["Instances"]*datagrouped["NombreJoursMois"].iloc[i]
                datagrouped["GainTotContrats"].iloc[i]=datagrouped["GainContratsJI"].iloc[i]*row_reference["Instances"]*datagrouped["NombreJoursMois"].iloc[i]

                # +datagrouped["GainContratsJI"].iloc[i]
                # datagrouped["GainTotOptims"].

                #Gain Décommissionnement
                
                if row_parcourue["Date"]<date_max:# and False:
                    if i<len(datagrouped)-1:
                        row_suivante=datagrouped.iloc[i+1].copy()
                        
                        
                    #déclencher le décom
                    
                    if ( i<len(datagrouped)-1 and (row_suivante["ResourceName"]!=row_parcourue["ResourceName"] or row_suivante["ResourceGroupName"]!=row_parcourue["ResourceGroupName"] )) or i==len(datagrouped)-1:
                        
                        # Add one month to the date using relativedelta
                        last_date = datetime.strptime(row_parcourue["Date"], "%Y-%m-%d")
                        row_decom=datagrouped.iloc[i].copy()
                        # print(row_decom["GainSizingJ"])
                        
                        row_decom["UsageTot"]=0.
                        row_decom["CostTot"]=0.
                        row_decom["UsageOnDemand"]=0.
                        row_decom["CostOnDemand"]=0.
                        row_decom["UsageReservation1Y"]=0.
                        row_decom["CostReservation1Y"]=0.
                        row_decom["InstancesBrut"]=0.
                        row_decom["CostJI"]=0.
                        row_decom["UsageJ"]=0.
                        row_decom["Instances"]=0.
                        row_decom["VariationJ"]=-row_reference["CostTot"].copy()/row_reference["NombreJoursMois"].copy()
                        # row_decom["VariationJI"]=-row_reference["CostTotJI"]
                        
                        row_decom["GainDecommissionnementJ"]=-row_parcourue["CostJ"].copy()
                        row_decom["GainJ"]=row_decom["GainJ"]+row_decom["GainDecommissionnementJ"]
                        row_decom["CostJ"]=0.


                        while last_date.strftime("%Y-%m-%d")<date_max: # ajout lignes jusqu'à date max
                            
                            new_date = (last_date + relativedelta(months=1))
                            
                            row_decom["Date"]=new_date.strftime("%Y-%m-%d")
                            row_decom["NombreJoursMois"]=get_count_days(new_date.strftime("%Y-%m-%d"))

                            row_decom["VariationTot"]=-row_reference["CostTot"].copy()/row_reference["NombreJoursMois"]*row_decom["NombreJoursMois"]
                            row_decom["GainTot"]=row_decom["GainJ"]*row_decom["NombreJoursMois"]



                            row_decom["GainTotSku"]=(row_decom["GainChangementSkuJI"]+row_decom["GainFonctionalitesSkuJI"])*row_reference["Instances"]*row_decom["NombreJoursMois"]
                            row_decom["GainTotSizing"]=row_decom["GainSizingJ"]*row_decom["NombreJoursMois"]
                            row_decom["GainTotReservation"]=row_decom["GainReservation1YJI"]*row_reference["Instances"]*row_decom["NombreJoursMois"]
                            row_decom["GainTotContrats"]=row_decom["GainContratsJI"]*row_reference["Instances"]*row_decom["NombreJoursMois"]
                            row_decom["GainTotDecom"]=row_decom["GainDecommissionnementJ"]*row_decom["NombreJoursMois"]
                                                                
                            last_date = new_date

                            datagrouped = pd.concat([datagrouped, pd.DataFrame([row_decom]).reset_index(drop=True)], ignore_index=True, axis=0).reset_index(drop=True)

                            i+=1
                        datagrouped=datagrouped.sort_values(["ResourceName","ResourcePath","ResourceGroupName","Config","Date"])

                row_parcourue=datagrouped.iloc[i].copy()
            i+=1
            
        index_instance+=1

    
    datagrouped["GainTot"]= datagrouped["GainJ"]*datagrouped["NombreJoursMois"]
    datagrouped["GainTotOptims"]=datagrouped["GainTotDecom"]+datagrouped["GainTotSku"]+datagrouped["GainTotContrats"]+datagrouped["GainTotSizing"]+datagrouped["GainTotReservation"]
    datagrouped["Ecart"]=datagrouped["GainTotOptims"]-datagrouped["GainTot"]
    # test
    datagrouped["GainJI"]=datagrouped["GainChangementSkuJI"]+datagrouped["GainReservation1YJI"]+datagrouped["GainContratsJI"]+datagrouped["GainFonctionalitesSkuJI"]

    return datagrouped

# test=get_gains_totaux_app_service(donnees, date_min,date_max, gains_premier_mois=False, resourceName="asp-xd-cmss-acc02-1")[["Date","ResourceName","ResourceGroupName","Instances","ResourceSku","CostTot","UsageTot","UsageOnDemand","CostOnDemand","UsageReservation1Y","CostReservation1Y","UnitCostOnDemand","UnitCostOnDemandNegocie","CostJ","VariationJ","GainJ","CostJI","VariationJI","GainJI","GainChangementSkuJI","GainReservation1YJI","GainContratsJI","GainSizingJ","GainDecommissionnementJ","VariationTot","GainTot","Ecart" ]]
# # test.loc[0,"ResourceGroupName"]

donnees=resourcesDevTest.copy()
date_min="2022-10-01"
date_max="2023-12-01"
resourceName="asp-xd-cmss-int03"
test=get_gains_totaux_app_service(donnees,date_min,date_max,gains_premier_mois=False,resourceName=resourceName)
# test=test[test["ResourceGroupName"]=="rg-xd-wsr-sfcm"]
test[["Date","NombreJoursMois","ProjectName","ApplicationName","LocationName","ServiceName","ResourcePath","ResourceGroupName","ResourceName","Instances","Config","ResourceSku","UsageTot","CostTot","UsageOnDemand","CostOnDemand","UsageReservation1Y","CostReservation1Y","UnitCostOnDemand","UnitCostOnDemandNegocie","UnitCostR1Y","VariationJ","GainJ","CostJI","VariationJI","GainJI","GainFonctionalitesSkuJI","GainChangementSkuJI","GainReservation1YJI","GainContratsJI","GainDevTestJI","GainSizingJ","GainDecommissionnementJ","VariationTot","GainTot","Ecart"]]

# %store gains_tot_app_service_Core_Model

# gains_tot_app_service_Core_Model.to_csv("C:\\Users\\hugo.dufrene\\Wavestone\\WE - ROLEX [CH] - Support mise en place Gouvernance Cloud public - 13665 - Documents partages\\Gouvernance Cloud\\FinOps\\Optimisations\\App Service\\20240112 - Analyse Gains Optims App Service.csv",index=False)

# plot_gains_tot(gains_tot_app_service_Core_Model.copy(),services=["Azure App Service"],date_min="2022-10-01")


# %%
def get_unit_prices_theo(partNumbers):
    # lignes=data_source[(data_source["Date"]==date)&(data_source["ResourcePath"]==row["ResourcePath"])]
    sum_usage=0
    sum_cost=0

    for ligne in lignes:
        
        pricing = azure_prices[(azure_prices["Date"]==date)&(azure_prices["partNumber"]==ligne["PartNumber"])]


        if not pricing.empty :
            if "Hour" in pricing["unitOfMeasure"].iloc[0]:
                hours=float(pricing["unitOfMeasure"].iloc[0].split(" ")[0])
            
            elif "Month" in pricing["unitOfMeasure"].iloc[0]:
                hours=float(pricing["unitOfMeasure"].iloc[0].split(" ")[0])*row["NombreJoursMois"]*24

            unitCostTheo = pricing["unitPrice"].iloc[0]/hours
        sum_cost=unitCostTheo*ligne["UsageTot"]
        
        return unitCostTheo

    else:
        print("ERREUR DAYS UNIT PRICE THEO",row["Date"],row["ResourceName"],row["ResourceName"],row["PartNumber"],row["ResourceSku"])
    return 1000

rowTest = datagrouped.iloc[11]
# get_unit_prices_theo(rowTest)

# %%
def get_all_part_numbers(row):
    data=resourcesDevTest.copy()
    data=data[(data["Date"]==row["Date"])&(data["ResourcePath"]==row["ResourcePath"])&(data["ServiceName"]==row["ServiceName"])]
    liste_part_numbers=[]
    for i in range(len(data)):
        for elt in data["PartNumber"].iloc[i]:
            # elt["costThero"]
            liste_part_numbers.append(elt)
    return liste_part_numbers

# get_all_part_numbers(rowTest)

# %%
def get_other_unit_cost_on_demand(data_source,row,partNumbers):
    unitCostOnDemand=0.
    
    # print(row["ResourceSku"])

    data=data_source[(data_source["ProjectName"]!=row["ProjectName"])&(data_source["LocationName"]==row["LocationName"])&(data_source["ServiceName"]==row["ServiceName"])&(data_source["UsageOnDemand"]>0)&(data_source["Date"]==row["Date"])].sort_values("Date",ascending=False)

    somme_cost=0
    somme_usage=0

    if not data.empty:
        # return data["PartNumber"]
        for partNumberI in partNumbers:
            # if partNumberI["pricingModel"]=="OnDemand":
            for j in range(len(data)):
                for partNumberJ in data["PartNumber"].iloc[j]:
                    if partNumberI["sku"]==partNumberJ["sku"] and partNumberJ["pricingModel"]=="OnDemand":
                        somme_cost+=partNumberJ["unitCost"]*partNumberI["usage"]
                        somme_usage+=partNumberI["usage"]
                        break
        # print(somme_usage)
        if somme_usage>0:
            return (somme_cost/somme_usage)

    return 0.0
partNumbers = get_all_part_numbers(rowTest)
get_other_unit_cost_on_demand(resourcesDevTest.copy(),rowTest,partNumbers)

# %%


# %%
azure_prices[azure_prices["meterName"]=="Premium v2 Plan P2 v2"]

# %%
def get_theo_cost_on_demand(datasource,row,partNumbers):
    

    pricing = azure_prices[(azure_prices["Date"]==row])]
    
    if not pricing.empty:
        
        format_unit_price(pricing[pricing["Date"]==row["Date"]]["unitPrice"].iloc[0],pricing[pricing["Date"]==row["Date"]]["unitOfMeasure"].iloc[0],row["Date"])

        unit_price_date=format_unit_price(pricing[pricing["Date"]==row["Date"]].iloc[0]) 

        lignes = datasource[(datasource["ResourceName"]==row["ResourceName"] )& (datasource["ResourcePath"]==row["ResourcePath"])&(datasource["ServiceName"]==row["ServiceName"])]
        
        somme_cost=0
        somme_usage=0

    # if not data.empty:
    #     # return data["PartNumber"]
    #     for partNumberI in partNumbers:
    #         # if partNumberI["pricingModel"]=="OnDemand":
    #         for j in range(len(data)):
    #             for partNumberJ in data["PartNumber"].iloc[j]:
    #                 if partNumberI["sku"]==partNumberJ["sku"] and partNumberJ["pricingModel"]=="OnDemand":
    #                     somme_cost+=partNumberJ["unitCost"]*partNumberI["usage"]
    #                     somme_usage+=partNumberI["usage"]
    #                     break
    #     # print(somme_usage)
    #     if somme_usage>0:
    #         return (somme_cost/somme_usage)

    return pricing[pricing["Date"]==row["Date"]]

get_theo_cost_on_demand(resourcesDevTest.copy(),rowTest,partNumbers)

# %%
rowTest

# %%
azure_prices[(azure_prices["meterName"]=="P2 v2 App")&(azure_prices["meterLocation"]=="EU West")&(azure_prices["partNumber"]=="AAA-43620")]

# %%
rowTest=datagrouped.iloc[11]
rowTest

# %%
liste_partnumbers_dev_test.append("AAG-02553" )
%store liste_partnumbers_dev_test

# %%
"AAG-02553" in liste_partnumbers_dev_test

# %%
def get_unit_prices_app_service(data_source,row):
    partNumbers = get_all_part_numbers(row)
    # print(partNumbers)
    # return (row["PartNumber"])  
    rabais = 0.11
    date=row["Date"]

    sum_usage_on_demand=0
    sum_usage_reservation=0
    sum_usage_dev_test=0

    sum_cost_on_demand=0
    sum_cost_reservation=0
    sum_cost_dev_test=0

    for partNumber in partNumbers:
        # print(partNumber)
        if partNumber["partNumber"] in liste_partnumbers_dev_test and row["ProjectName"]=="IT-DEV":
            partNumber["pricingModel"]="DevTest"

        if partNumber["pricingModel"]=="OnDemand":
            # print(partNumber)
            if not "SSL Connections IP SSL Connection" in partNumber["sku"]:
                sum_usage_on_demand+=partNumber["usage"]
                sum_cost_on_demand+=partNumber["cost"]

        elif "Reservation" in partNumber["pricingModel"]:
            sum_usage_reservation+=partNumber["usage"]
            sum_cost_reservation+=partNumber["cost"]

        elif partNumber["pricingModel"]=="DevTest":
            # print(partNumber)
            sum_usage_dev_test+=partNumber["usage"]
            sum_cost_dev_test+=partNumber["cost"]

        else:
            # print(partNumber)
            print("ERREUR")


    unitCostOnDemand=sum_cost_on_demand/max(sum_usage_on_demand,0.01)

    # print(sum_cost_on_demand,sum_usage_on_demand)

    unitCostReservation=sum_cost_reservation/max(sum_usage_reservation,0.01)
    unitCostDevTest=sum_cost_dev_test/max(sum_usage_dev_test,0.01)

    if unitCostOnDemand==0:
        unitCostOnDemand = get_other_unit_cost_on_demand(data_source,row,partNumbers)
    # if unitCostOnDemand==0:
    #     unitCostOnDemand = get_theo_cost_on_demand(datasource,row,partNumbers)
    if row["Date"]>="2023-09-01":
        unitCostOnDemand=unitCostOnDemand/(1-rabais)
        unitCostOnDemandNego=unitCostOnDemand*(1-rabais)
    else:
        unitCostOnDemandNego=unitCostOnDemand

    # if unitCostDevTest==0:
    #     unitCostDevTest=unitCostOnDemandNego
    return unitCostOnDemand,unitCostOnDemandNego,unitCostReservation,unitCostDevTest


    
rowTest = datagrouped.iloc[7]
get_unit_prices_app_service(resourcesDevTest.copy(),rowTest)

# %%
rowTest = datagrouped.iloc[7]
get_unit_prices_app_service(resourcesDevTest.copy(),rowTest)

# %%
def get_gains_totaux_app_service(donnees,date_min,date_max,gain_contrat=True,gains_premier_mois=True,resourceName=None,azure_prices=None):
    # date_max=sorted(list(donnees["Date"].unique()))[-1]

    donnees=donnees[donnees["ServiceName"]=="Azure App Service"]
    azure_prices=azure_prices[azure_prices["meterCategory"]=="Azure App Service"]

    if not resourceName==None:
        donnees=donnees[(donnees["ResourceName"]==resourceName)]
    # gains_tot_sql_database_CoreModel[gains_tot_sql_database_CoreModel["ResourceName"]=="xd-cmss-dev"]
    # donnees=donnees[(donnees["ProjectName"]=='IT-PROD-IT')]
    
    data=donnees.groupby(["Date","ResourceName","ProjectName","ApplicationName","LocationName","ServiceName","ResourceGroupName","ResourcePath","ServiceTier","ResourceSkuTier","ResourceSkuSize"])[["UsageTot","UsageOnDemand","CostTot","CostOnDemand","UsageReservation1Y","CostReservation1Y","UsageDevTest","CostDevTest"]].sum().reset_index()    

    
    # return data
    gains=None
 
    data = data.fillna(0)

    data["Config"] = data.apply(lambda row: get_config(row), axis=1)

    data["ResourceSku"]=data.apply(lambda row:row["ResourceSkuTier"]+" "+row["ResourceSkuSize"],axis=1)
    

    # data["InstancesBrut"]=data.apply(lambda row:get_nb_instances("Azure App Service",row,brut=True),axis=1)
    data["Instances"]=data.apply(lambda row:get_nb_instances("Azure App Service",row,brut=False),axis=1)

    # data["UsageReservation1YBrut"]=data["UsageReservation1Y"].copy()
    # data["UsageOnDemandBrut"]=data["UsageOnDemand"].copy()


    data_source=data.copy()

    data_source["UnitCostOnDemand"]=0.0
    data_source["UnitCostOnDemandNegocie"]=0.0
    

    data=data[data["Date"]>=date_min]
    data["NombreJoursMois"]=data["Date"].apply(lambda row:get_count_days(row))
    
    

    # data=data[data["Config"]=="Compute"]
    # return data
    
    datagrouped=data.groupby(["Date","NombreJoursMois","ResourceName","ProjectName","ApplicationName","LocationName","ServiceName","ResourceGroupName","ResourcePath","Config"])[["UsageTot","CostTot","UsageOnDemand","CostOnDemand","CostReservation1Y","UsageReservation1Y","UsageDevTest","CostDevTest","Instances"]].sum().reset_index()

    
    datagrouped["ResourceSku"] = datagrouped.apply(lambda row: list(data[
        (data["Date"] == row["Date"]) &
        (data["ResourcePath"] == row["ResourcePath"])&(data["Config"]==row["Config"])    ]["ResourceSku"].unique()), axis=1) 

    # datagrouped["PartNumbers"] = datagrouped.apply(lambda row: list(data[
    #     (data["Date"] == row["Date"]) &
    #     (data["ResourceName"] == row["ResourceName"])&(data["Config"]==row["Config"])    ]["PartNumber"].unique()), axis=1) 
    # return datagrouped.sort_values(["ResourceName","ResourcePath","ResourceGroupName","Config","Date"])
        # data["NombreJoursMois"]=data["Date"].apply(lambda row:get_count_days(row))


    datagrouped[["UnitCostOnDemand","UnitCostOnDemandNego","UnitCostReservation1Y","UnitCostDevTest"]]=datagrouped.apply(
        lambda row :pd.Series(get_unit_prices_app_service(resourcesDevTest.copy(),row)),
        axis=1
    )


    # datagrouped=data.copy()

    datagrouped["CostJ"]=datagrouped["CostTot"]/datagrouped["NombreJoursMois"]
    datagrouped["CostSuppJI"]=0.0
    datagrouped["CostSuppJ"]=0.0
    datagrouped["CostCompJI"]=0.0
    datagrouped["CostCompJ"]=0.0
    datagrouped["CostCompTot"]=0.0
    datagrouped["UsageJ"]=datagrouped["UsageTot"]/datagrouped["NombreJoursMois"]

    datagrouped["CostJI"]=datagrouped["CostJ"]/datagrouped["Instances"]
    datagrouped["UsageJI"]=datagrouped["UsageJ"]/datagrouped["Instances"]

    datagrouped["VariationJ"]=0.
    datagrouped["VariationJI"]=0.
    datagrouped["VariationTot"]=0.

    datagrouped["VariationCompJ"]=0.
    datagrouped["VariationCompJI"]=0.
    datagrouped["VariationCompTot"]=0.

    datagrouped["GainFonctionalitesSkuJI"]=0.
    datagrouped["GainChangementSkuJI"]=0.
    datagrouped["GainReservation1YJI"]=0.
    datagrouped["GainContratsJI"]=0.
    datagrouped["GainDevTestJI"]=0.
    datagrouped["GainSuppJI"]=0.
    datagrouped["GainSuppJ"]=0.
    datagrouped["GainJI"]=0.
    datagrouped["GainCompJI"]=0.
    datagrouped["GainCompJ"]=0.
    


    datagrouped["GainSizingJ"]=0.
    datagrouped["GainSizingCompJ"]=0.

    datagrouped["GainDecommissionnementJ"]=0.
    datagrouped["GainJ"]=0.


    datagrouped["GainTotDecom"]=0.
    datagrouped["GainTotSku"]=0.
    datagrouped["GainTotSkuComp"]=0.
    datagrouped["GainTotSizing"]=0.
    datagrouped["GainTotReservation"]=0.
    datagrouped["GainTotContrats"]=0.
    datagrouped["GainTotSizingComp"]=0.
    datagrouped["GainTotDevTest"]=0.
    datagrouped["GainTotComp"]=0.


    datagrouped["GainTotOptims"]=0.
    datagrouped["GainTot"]=0.

    
    
    datagrouped= datagrouped.sort_values(["ResourceName","ResourcePath","ResourceGroupName","Config","Date"])

    index_instance=0

    # return datagrouped

    while index_instance<len(datagrouped)-1:
        row_reference=datagrouped.iloc[index_instance]
        gainJ_reference=0
        i=index_instance+0

        row_parcourue=datagrouped.iloc[i].copy()
          
        date_min=datagrouped[(datagrouped["ResourcePath"]==row_reference["ResourcePath"])&(datagrouped["ResourceGroupName"]==row_reference["ResourceGroupName"])].sort_values("Date")["Date"].iloc[0]


        while i<len(datagrouped) and row_reference["Date"]==date_min and  row_parcourue["Config"]=="Compute" and row_parcourue["ResourcePath"]==row_reference["ResourcePath"] and row_parcourue["ResourceGroupName"]==row_reference["ResourceGroupName"]:
            row_parcourue=datagrouped.iloc[i].copy()

            if i==index_instance:
                print(row_parcourue["ResourceName"])

            if row_parcourue["UnitCostOnDemand"]==0 and i>index_instance and sorted(row_parcourue["ResourceSku"])==sorted(datagrouped.iloc[i-1]["ResourceSku"]) and row_parcourue["ResourcePath"]==datagrouped.iloc[i-1]["ResourcePath"] and row_parcourue["ResourceGroupName"]==datagrouped.iloc[i-1]["ResourceGroupName"]:
                unitCostOnDemand=datagrouped["UnitCostOnDemand"].iloc[i-1]
                unitCostOnDemandNego=datagrouped["UnitCostOnDemandNego"].iloc[i-1]

                if row_parcourue["Date"]>="2023-09-01" and datagrouped["Date"].iloc[i-1]<"2023-08-01":
                    unitCostOnDemandNego=unitCostOnDemandNego*(1-0.11)

                datagrouped["UnitCostOnDemand"].iloc[i]=unitCostOnDemand
                datagrouped["UnitCostOnDemandNego"].iloc[i]=unitCostOnDemandNego

            row_parcourue=datagrouped.iloc[i].copy()

            # print(i,row_parcourue["Date"],row_parcourue["ResourceGroupName"],row_parcourue["ResourceName"])

            if row_parcourue["UsageTot"]>0 and row_parcourue["ResourceName"]==row_reference["ResourceName"] and row_parcourue["ResourceGroupName"]==row_reference["ResourceGroupName"]:
                
                #Gain fonctionalités
                if row_reference["UnitCostOnDemand"]>0  and row_parcourue["UnitCostOnDemand"]>0 and sorted(row_reference["ResourceSku"])==sorted(row_parcourue["ResourceSku"]):
                    datagrouped["GainFonctionalitesSkuJI"].iloc[i]=24*(row_parcourue["UnitCostOnDemand"]-row_reference["UnitCostOnDemand"]) #*row_parcourue["UsageTot"]/(row_parcourue["NombreJoursMois"])
                    # datagrouped["GainFonctionalitesSkuJI"].iloc[i]=0
                
                #Gain changement SKU
                if row_reference["UnitCostOnDemand"]>0  and sorted(row_reference["ResourceSku"])!=sorted(row_parcourue["ResourceSku"]): #pas changement nb instances, pas changement sku, pas réservation
                    datagrouped["GainChangementSkuJI"].iloc[i]=24*(row_parcourue["UnitCostOnDemand"]-row_reference["UnitCostOnDemand"])
                


                #Gain Contrats
                if row_parcourue["UnitCostOnDemand"]!=row_parcourue["UnitCostOnDemandNego"] and gain_contrat:
                    datagrouped["GainContratsJI"].iloc[i]=24*(row_parcourue["UnitCostOnDemandNego"]-row_parcourue["UnitCostOnDemand"])  # *row_parcourue["UsageOnDemand"]/row_parcourue["UsageTot"]

                #Gain Reservation
                # if row_parcourue["UsageReservation1Y"]>0: #pas changement nb instances, pas changement sku, pas contrat
                datagrouped["GainReservation1YJI"].iloc[i]=24*(row_parcourue["UnitCostReservation1Y"]-row_parcourue["UnitCostOnDemandNego"])*row_parcourue["UsageReservation1Y"]/row_parcourue["UsageTot"]

                #Gain Dev Test
                if row_parcourue["ProjectName"]=="IT-DEV" and row_parcourue["Date"]>="2023-11-01":
                    datagrouped["GainDevTestJI"].iloc[i]=24*(row_parcourue["UnitCostDevTest"]-row_parcourue["UnitCostOnDemandNego"]) *row_parcourue["UsageDevTest"]/row_parcourue["UsageTot"]
                
                
                #Gain functionalité supp
                datagrouped["CostSuppJ"].iloc[i]=datagrouped[(datagrouped["Config"]!="Compute")&(datagrouped["ResourcePath"]==row_reference["ResourcePath"])&(datagrouped["ResourceGroupName"]==row_reference["ResourceGroupName"])&(datagrouped["Date"]==row_parcourue["Date"])]["CostJ"].sum()
                datagrouped["CostSuppJI"].iloc[i]=datagrouped["CostSuppJ"].iloc[i] / datagrouped["Instances"].iloc[i]
                datagrouped["CostCompJI"].iloc[i] =datagrouped["CostJI"].iloc[i]+datagrouped["CostSuppJI"].iloc[i]
                datagrouped["CostCompJ"].iloc[i] =datagrouped["CostJ"].iloc[i]+datagrouped["CostSuppJ"].iloc[i]
                datagrouped["CostCompTot"].iloc[i] =datagrouped["CostCompJ"].iloc[i] * datagrouped["NombreJoursMois"].iloc[i] 

                if i==index_instance:
                    row_reference=datagrouped.iloc[index_instance].copy()

                datagrouped["GainSuppJI"].iloc[i]=datagrouped["CostSuppJI"].iloc[i]-row_reference["CostSuppJI"]
                row_parcourue=datagrouped.iloc[i].copy()


                #Gain Sizing
                if row_reference["UnitCostOnDemand"]>0 : #pas changement nb instances, pas changement sku, pas contrat
                    datagrouped["GainSizingJ"].iloc[i]=24*(row_parcourue["Instances"]-row_reference["Instances"])*row_parcourue["CostTot"]/row_parcourue["UsageTot"]
                    datagrouped["GainSizingCompJ"].iloc[i]=24*(row_parcourue["Instances"]-row_reference["Instances"])*row_parcourue["CostCompTot"]/row_parcourue["UsageTot"]

                    # datagrouped["GainSizingJ"].iloc[i]=0

                
                    # datagrouped["GainReservation1YJI"].iloc[i]=0
                

                #Gain Dev Test
                if row_parcourue["ProjectName"]=="IT-DEV" and row_parcourue["Date"]>="2023-11-01":
                    datagrouped["GainDevTestJI"].iloc[i]=24*(row_parcourue["UnitCostDevTest"]-row_parcourue["UnitCostOnDemandNego"]) *row_parcourue["UsageDevTest"]/row_parcourue["UsageTot"]

                   
                datagrouped["VariationJI"].iloc[i]= datagrouped["CostJI"].iloc[i]-row_reference["CostJI"]
                datagrouped["VariationJ"].iloc[i]= datagrouped["CostJ"].iloc[i]-row_reference["CostJ"]
                datagrouped["VariationTot"].iloc[i]= (datagrouped["CostJ"].iloc[i]-row_reference["CostJ"])*datagrouped["NombreJoursMois"].iloc[i]

                datagrouped["VariationCompJI"].iloc[i]= datagrouped["CostCompJI"].iloc[i]-row_reference["CostCompJI"]
                datagrouped["VariationCompJ"].iloc[i]= datagrouped["CostCompJ"].iloc[i]-row_reference["CostCompJ"]
                datagrouped["VariationCompTot"].iloc[i]= (datagrouped["CostCompJ"].iloc[i]-row_reference["CostCompJ"])*datagrouped["NombreJoursMois"].iloc[i]
                
                datagrouped["GainJI"].iloc[i]=datagrouped["GainChangementSkuJI"].iloc[i]+datagrouped["GainReservation1YJI"].iloc[i]+datagrouped["GainContratsJI"].iloc[i]+datagrouped["GainFonctionalitesSkuJI"].iloc[i]+datagrouped["GainDevTestJI"].iloc[i]
                datagrouped["GainCompJI"].iloc[i]=datagrouped["GainChangementSkuJI"].iloc[i]+datagrouped["GainReservation1YJI"].iloc[i]+datagrouped["GainContratsJI"].iloc[i]+datagrouped["GainFonctionalitesSkuJI"].iloc[i]+datagrouped["GainDevTestJI"].iloc[i]+datagrouped["GainSuppJI"].iloc[i]

                if i==index_instance and not gains_premier_mois:
                    gainJ_reference=(datagrouped["GainJI"].iloc[i])*row_reference["Instances"]+datagrouped["GainSizingJ"].iloc[i]
                    gainJ_reference_comp=(datagrouped["GainCompJI"].iloc[i])*row_reference["Instances"]+datagrouped["GainSizingCompJ"].iloc[i]

                datagrouped["GainJ"].iloc[i]=datagrouped["GainJI"].iloc[i]*row_reference["Instances"]+datagrouped["GainSizingJ"].iloc[i]-gainJ_reference
                datagrouped["GainCompJ"].iloc[i]=datagrouped["GainCompJI"].iloc[i]*row_reference["Instances"]+datagrouped["GainSizingCompJ"].iloc[i]-gainJ_reference_comp

                
                datagrouped["GainTotSku"].iloc[i]=(datagrouped["GainChangementSkuJI"].iloc[i]+datagrouped["GainFonctionalitesSkuJI"].iloc[i])*row_reference["Instances"]*datagrouped["NombreJoursMois"].iloc[i]
                datagrouped["GainTotSkuComp"].iloc[i]=(datagrouped["GainChangementSkuJI"].iloc[i]+datagrouped["GainFonctionalitesSkuJI"].iloc[i]+datagrouped["GainSuppJI"].iloc[i])*row_reference["Instances"]*datagrouped["NombreJoursMois"].iloc[i]
                datagrouped["GainTotSizing"].iloc[i]=datagrouped["GainSizingJ"].iloc[i]*datagrouped["NombreJoursMois"].iloc[i]
                datagrouped["GainTotSizingComp"].iloc[i]=datagrouped["GainSizingCompJ"].iloc[i]*datagrouped["NombreJoursMois"].iloc[i]

                datagrouped["GainTotReservation"].iloc[i]=datagrouped["GainReservation1YJI"].iloc[i]*row_reference["Instances"]*datagrouped["NombreJoursMois"].iloc[i]
                datagrouped["GainTotContrats"].iloc[i]=datagrouped["GainContratsJI"].iloc[i]*row_reference["Instances"]*datagrouped["NombreJoursMois"].iloc[i]
                datagrouped["GainTotDevTest"].iloc[i]=datagrouped["GainDevTestJI"].iloc[i]*row_reference["Instances"]*datagrouped["NombreJoursMois"].iloc[i]

                # datagrouped["GainTotSupp"].iloc[i]=datagrouped["GainContratsJI"].iloc[i]*row_reference["Instances"]*datagrouped["NombreJoursMois"].iloc[i]

                # datagrouped["GainTotSupp"].iloc[i]=datagrouped["GainSuppJI"].iloc[i]*row_reference["Instances"]*datagrouped["NombreJoursMois"].iloc[i]

                if i==index_instance:
                    row_reference=datagrouped.iloc[index_instance].copy()

                # +datagrouped["GainContratsJI"].iloc[i]
                # datagrouped["GainTotOptims"].

                #Gain Décommissionnement
                if row_parcourue["Date"]<date_max:# and False:
                    if i<len(datagrouped)-1:
                        row_suivante=datagrouped.iloc[i+1].copy()
                        
                        
                    #déclencher le décom
                    
                    if ( i<len(datagrouped)-1 and (row_suivante["ResourceName"]!=row_parcourue["ResourceName"] or row_suivante["ResourceGroupName"]!=row_parcourue["ResourceGroupName"] )) or i==len(datagrouped)-1:
                        
                        # Add one month to the date using relativedelta
                        last_date = datetime.strptime(row_parcourue["Date"], "%Y-%m-%d")
                        row_decom=datagrouped.iloc[i].copy()
                        # print(row_decom["GainSizingJ"])
                        
                        row_decom["UsageTot"]=0.
                        row_decom["CostTot"]=0.
                        row_decom["UsageOnDemand"]=0.
                        row_decom["CostOnDemand"]=0.
                        row_decom["UsageReservation1Y"]=0.
                        row_decom["CostReservation1Y"]=0.
                        row_decom["InstancesBrut"]=0.
                        row_decom["CostJI"]=0.
                        row_decom["CostSuppJ"]=0.
                        row_decom["CostSuppJI"]=0.
                        row_decom["CostCompJI"]=0.
                        row_decom["CostCompJ"]=0.
                        row_decom["CostCompTot"]=0.


                        row_decom["UsageJ"]=0.
                        row_decom["Instances"]=0.
                        row_decom["VariationJ"]=-row_reference["CostTot"].copy()/row_reference["NombreJoursMois"].copy()
                        row_decom["VariationCompJ"]=-row_reference["CostCompTot"].copy()/row_reference["NombreJoursMois"].copy()
                        
                        row_decom["GainDecommissionnementJ"]=-row_parcourue["CostJ"].copy()

                        row_decom["GainJ"]=row_decom["GainJ"]+row_decom["GainDecommissionnementJ"]
                        row_decom["GainCompJ"]=row_decom["GainCompJ"]+row_decom["GainDecommissionnementJ"]

                        row_decom["CostJ"]=0.
                        row_decom["CostCompJ"]=0.

                        while last_date.strftime("%Y-%m-%d")<date_max: # ajout lignes jusqu'à date max
                            
                            new_date = (last_date + relativedelta(months=1))
                            
                            row_decom["Date"]=new_date.strftime("%Y-%m-%d")
                            row_decom["NombreJoursMois"]=get_count_days(new_date.strftime("%Y-%m-%d"))

                            row_decom["VariationTot"]=-row_reference["CostTot"].copy()/row_reference["NombreJoursMois"]*row_decom["NombreJoursMois"]
                            row_decom["VariationCompTot"]=-row_reference["CostCompTot"].copy()/row_reference["NombreJoursMois"]*row_decom["NombreJoursMois"]

                            row_decom["GainTot"]=row_decom["GainJ"]*row_decom["NombreJoursMois"]
                            row_decom["GainCompTot"]=row_decom["GainCompJ"]*row_decom["NombreJoursMois"]

                            row_decom["GainTotSku"]=(row_decom["GainChangementSkuJI"]+row_decom["GainFonctionalitesSkuJI"])*row_reference["Instances"]*row_decom["NombreJoursMois"]
                            row_decom["GainTotSkuComp"]=(row_decom["GainChangementSkuJI"]+row_decom["GainFonctionalitesSkuJI"]+row_decom["GainSuppJI"])*row_reference["Instances"]*row_decom["NombreJoursMois"]

                            row_decom["GainTotSizing"]=row_decom["GainSizingJ"]*row_decom["NombreJoursMois"]
                            row_decom["GainTotSizingComp"]=row_decom["GainSizingCompJ"]*row_decom["NombreJoursMois"]

                            row_decom["GainTotReservation"]=row_decom["GainReservation1YJI"]*row_reference["Instances"]*row_decom["NombreJoursMois"]
                            row_decom["GainTotContrats"]=row_decom["GainContratsJI"]*row_reference["Instances"]*row_decom["NombreJoursMois"]
                            row_decom["GainTotDevTest"]=row_decom["GainDevTestJI"]*row_reference["Instances"]*row_decom["NombreJoursMois"]
                            row_decom["GainTotDecom"]=row_decom["GainDecommissionnementJ"]*row_decom["NombreJoursMois"]
                            # datagrouped["GainTotDevTest"].iloc[i]=datagrouped["GainDevTestJI"].iloc[i]*row_reference["Instances"]*datagrouped["NombreJoursMois"].iloc[i]

                            # row_decom["GainTotSupp"]=row_decom["GainSuppJ"]*row_decom["NombreJoursMois"]     

                            last_date = new_date

                            datagrouped = pd.concat([datagrouped, pd.DataFrame([row_decom]).reset_index(drop=True)], ignore_index=True, axis=0).reset_index(drop=True)

                            i+=1
                        datagrouped=datagrouped.sort_values(["ResourceName","ResourcePath","ResourceGroupName","Config","Date"])



               
                row_parcourue=datagrouped.iloc[i].copy()
            i+=1
            
        index_instance+=1

    # datagrouped[datagrouped["Config"]=="Compute"]



    datagrouped["GainTot"]= datagrouped["GainJ"]*datagrouped["NombreJoursMois"]
    datagrouped["GainTotComp"]= datagrouped["GainCompJ"]*datagrouped["NombreJoursMois"]


    datagrouped["GainTotOptimsComp"]=datagrouped["GainTotDecom"]+datagrouped["GainTotSkuComp"]+datagrouped["GainTotContrats"]+datagrouped["GainTotSizingComp"]+datagrouped["GainTotReservation"]+datagrouped["GainTotDevTest"]
    
    datagrouped["Ecart"]=datagrouped["VariationTot"]-datagrouped["GainTot"]
    datagrouped["EcartComp"]=datagrouped["VariationCompTot"]-datagrouped["GainTotComp"]
    datagrouped["EcartGainsOptimComp"]=datagrouped["GainTotComp"]-datagrouped["GainTotOptimsComp"]
    # datagrouped["GainJI"]=datagrouped["GainChangementSkuJI"]+datagrouped["GainReservation1YJI"]+datagrouped["GainContratsJI"]+datagrouped["GainFonctionalitesSkuJI"]

    return datagrouped [datagrouped["Config"]=="Compute"]

# test=get_gains_totaux_app_service(donnees, date_min,date_max, gains_premier_mois=False, resourceName="asp-xd-cmss-acc02-1")[["Date","ResourceName","ResourceGroupName","Instances","ResourceSku","CostTot","UsageTot","UsageOnDemand","CostOnDemand","UsageReservation1Y","CostReservation1Y","UnitCostOnDemand","UnitCostOnDemandNegocie","CostJ","VariationJ","GainJ","CostJI","VariationJI","GainJI","GainChangementSkuJI","GainReservation1YJI","GainContratsJI","GainSizingJ","GainDecommissionnementJ","VariationTot","GainTot","Ecart" ]]
# # test.loc[0,"ResourceGroupName"]

donnees=resourcesDevTest.copy()
date_min="2022-10-01"
date_max="2024-01-01"
resourceName=None

gains_tot_app_service_Core_Model=get_gains_totaux_app_service(donnees,date_min,date_max,gains_premier_mois=False,resourceName=resourceName,azure_prices=azure_prices)
%store gains_tot_app_service_Core_Model
gains_tot_app_service_Core_Model.to_excel("C:\\Users\\hugo.dufrene\\Wavestone\\WE - ROLEX [CH] - Support mise en place Gouvernance Cloud public - 13665 - Documents partages\\Gouvernance Cloud\\FinOps\\Optimisations\\App Service\\20240204 - Analyse Gains Optims App Service.xlsx",index=False)
gains_tot_app_service_Core_Model

# datagrouped[(abs(datagrouped["Ecart"])>0.1)|(abs(datagrouped["EcartComp"])>0.1)|(abs(datagrouped["EcartGainsOptimComp"])>0.1)]  [["Date","NombreJoursMois","ResourceGroupName","ResourceName","Instances","ResourceSku","UsageTot","UsageOnDemand","UsageReservation1Y","UsageDevTest","GainFonctionalitesSkuJI","GainChangementSkuJI","GainReservation1YJI","GainContratsJI","GainDevTestJI","GainSuppJI","GainJI","VariationJI","GainCompJI","VariationCompJI",
# "GainSizingJ","GainSizingCompJ","GainDecommissionnementJ","GainJ","VariationJ","GainCompJ","VariationCompJ",
# "VariationCompTot","GainTotComp","GainTotOptimsComp","Ecart","EcartComp","EcartGainsOptimComp"
# ]]

# "VariationJ","CostCompJ","VariationCompJ",	,"CostJI","VariationJI","GainJI","CostSuppJI","CostCompJI",	"VariationCompJI",	"GainCompJI"	,"VariationTot","GainTot","VariationCompTot","GainTotComp","GainTotOptimsComp","Ecart","EcartComp","EcartGainsOptimComp"]]

# # %store gains_tot_app_service_Core_Model

# plot_gains_tot(gains_tot_app_service_Core_Model.copy(),services=["Azure App Service"],date_min="2022-10-01")


# %%


# %%
resourcesDevTest[(resourcesDevTest["Date"]=="2023-08-01")&(resourcesDevTest["ResourceGroupName"]=="rg-kr-cmss-test")&(resourcesDevTest["ResourceName"]=="asp-kr-cmss-supp01")]["PartNumber"].iloc[0]

# %%
datagrouped.to_excel("C:\\Users\\hugo.dufrene\\Wavestone\\WE - ROLEX [CH] - Support mise en place Gouvernance Cloud public - 13665 - Documents partages\\Gouvernance Cloud\\FinOps\\Optimisations\\App Service\\20240204 - Analyse Gains Optims App Service.xlsx",index=False)

# %%
gains_tot_virtual_machines_CoreModel

# %%
datagrouped[datagrouped["Config"]!="Compute"]

# %%
%store datagrouped

# %%
datagrouped

# %%
datagrouped.to_excel("C:\\Users\\hugo.dufrene\\Wavestone\\WE - ROLEX [CH] - Support mise en place Gouvernance Cloud public - 13665 - Documents partages\\Gouvernance Cloud\\FinOps\\Optimisations\\App Service\\20240112 - Analyse Gains Optims App Service.xlsx",index=False)


# %%
resourcesDevTest[resourcesDevTest["ResourceName"]=="asp-xd-cmss-acc01"]

# %%
datagrouped[["Date","NombreJoursMois","ProjectName","ApplicationName","LocationName","ServiceName","ResourcePath","ResourceGroupName","ResourceName","Instances","Config","ResourceSku","UsageTot","CostTot","UsageOnDemand","CostOnDemand","UsageReservation1Y","CostReservation1Y","UnitCostOnDemand","UnitCostOnDemandNego","UnitCostR1Y","VariationJ","GainJ","CostJI","VariationJI","GainJI","GainFonctionalitesSkuJI","GainChangementSkuJI","GainReservation1YJI","GainContratsJI","GainDevTestJI","GainSizingJ","GainDecommissionnementJ","VariationTot","GainTot","Ecart"]]


# %%
datagrouped["Config"].unique()

# %%
azure_prices[azure_prices["meterCategory"]=="Azure App Service"]

# %%
resourcesDevTest

# %%
test["ResourcePath"].iloc[0]

# %%
search_lignes_facturations(lignes_Facturation_DevTest,"Azure App Service",test["ResourcePath"].iloc[0],"2023-09-01")

# %%
%store -r azure_prices

# %%
resourcesDevTest[(resourcesDevTest["ServiceName"]=="Azure App Service")&(resourcesDevTest["ResourceName"]==resourceName)]

# %%
resourcesDevTest[(resourcesDevTest["CostOnDemand"]>0)&(resourcesDevTest["Date"]>="2023-12-01")&(resourcesDevTest["ServiceName"]=="Azure App Service")]

# %%
resourcesDevTest[resourcesDevTest["ResourceName"]=="asp-xd-cmss-demo01"]

# %%
def get_gains_totaux_virtual_machines(donnees,date_min,gain_contrat=True,gains_premier_mois=True,resourceName=None):
    donnees=donnees[donnees["ServiceName"]=="Virtual Machines"]
    
    if not resourceName==None:
        donnees=donnees[(donnees["ResourceName"]==resourceName)]
    # gains_tot_sql_database_CoreModel[gains_tot_sql_database_CoreModel["ResourceName"]=="xd-cmss-dev"]
    # donnees=donnees[(donnees["ProjectName"]=='IT-PROD-IT')]
    
    data=donnees.groupby(["Date","ResourceName","ProjectName","ApplicationName","LocationName","ServiceName","ResourceGroupName","ResourcePath","ServiceTier","ResourceSkuTier","ResourceSkuSize"])[["UsageTot","UsageOnDemand","CostTot","CostOnDemand","CostReservation1Y","UsageReservation1Y"]].sum().reset_index()    

    
    # return data
    gains=None
 
    data = data.fillna(0)
    # return data
    data["Config"] = data.apply(lambda row: get_config(row), axis=1)
    # data["Capacité"]=data.apply(lambda row:get_nb_vcore(row),axis=1)
    # return data
    data["ResourceSku"]=data.apply(lambda row:row["ResourceSkuTier"]+" "+row["ResourceSkuSize"],axis=1)
    
    # data["UsageTot"]=data.apply(lambda row:row["UsageTot"]*24 if "DTU" in "ResourceSkuSize" else row["UsageTot"],axis=1)
    
    # data["UsageOnDemand"]=data.apply(lambda row:row["UsageOnDemand"]*24 if "DTU" in row["ResourceSkuSize"] else row["UsageOnDemand"],axis=1)
    data["InstancesBrut"]=data.apply(lambda row:get_nb_instances("Virtual Machines",row,brut=True),axis=1)
    data["Instances"]=data.apply(lambda row:get_nb_instances("Virtual Machines",row,brut=False),axis=1)

    # data["UsageTotBrut"] = data.apply(lambda row: row["UsageTot"] * 24 if "DTU" in row["ResourceSkuSize"] else row["UsageTot"], axis=1)
    # data["UsageTot"] = data.apply(lambda row: row["UsageTot"] * 24 if "DTU" in row["ResourceSkuSize"] else row["UsageTot"]/max(row["Capacité"],1), axis=1)
    data["UsageReservation1YBrut"]=data["UsageReservation1Y"].copy()
    data["UsageOnDemandBrut"]=data["UsageOnDemand"].copy()

    # data["UsageOnDemand"] = data.apply(lambda row:row["UsageOnDemand"] * 24 if "DTU" in row["ResourceSkuSize"] else row["UsageOnDemand"]/max(row["Capacité"],1),axis=1)
    # data["UsageReservation1Y"] = data.apply(lambda row: row["UsageReservation1Y"] * 24 if "DTU" in row["ResourceSkuSize"] else row["UsageReservation1Y"]/max(row["Capacité"],1), axis=1)

    # return data

    data_source=data.copy()
    # data_source = donnees.groupby(["Date","ProjectName","ServiceName","ResourceName"])[["CostTot","UsageTot","CostOnDemand","UsageOnDemand","CostReservation1Y","UsageReservation1Y"]].sum().reset_index()
    data_source["UnitCostOnDemand"]=0.0
    data_source["UnitCostOnDemandNegocie"]=0.0
    
    data=data[data["Date"]>=date_min]
    
    # return data

#     data_source["UsageTot"] =data_source.apply(lambda row:row["UsageTot"] * 24 if "DTU" in row["ResourceSkuSize"] else row["UsageTot"]/max(row["Capacité"],1),axis=1)
# data_source["UsageOnDemand"] =data_source.apply(lambda row:row["UsageOnDemand"] * 24 if "DTU" in row["ResourceSkuSize"] else row["UsageOnDemand"]/max(row["Capacité"],1),axis=1)
    # return data
    
    datagrouped=data.groupby(["Date","ResourceName","ProjectName","ApplicationName","LocationName","ServiceName","ResourceGroupName","ResourcePath","Config"])[["UsageTot","CostTot","UsageOnDemand","UsageOnDemandBrut","CostOnDemand","CostReservation1Y","UsageReservation1Y","UsageReservation1YBrut","Instances","InstancesBrut"]].sum().reset_index()

    datagrouped["ResourceSku"] = datagrouped.apply(lambda row: list(data[
        (data["Date"] == row["Date"]) &
        (data["ResourceName"] == row["ResourceName"])&(data["Config"]==row["Config"])    ]["ResourceSku"].unique()), axis=1) 
    
    datagrouped["UnitCostOnDemand"]=0.
    datagrouped["UnitCostOnDemandNegocie"]=0.
    datagrouped["UnitCostR1Y"]=0.
    # return data_source
    
    print("Pré traitement 1 terminé")
    # return datagrouped

    datagrouped[["UnitCostOnDemand", "FacteurRabaisNegociationContrat"]] = datagrouped.apply(
        lambda row: pd.Series(get_unit_cost_on_demand_v2(data_source, row, "Virtual Machines", negocie=False)),
        axis=1
    )
    
    print("Pré traitement 2 terminé")

    
    datagrouped["UnitCostOnDemandNegocie"]=datagrouped["UnitCostOnDemand"]*datagrouped["FacteurRabaisNegociationContrat"]
    
    # datagrouped=dataTest.sort_values(["ResourceName","ResourcePath","Date"])
    datagrouped["NombreJoursMois"]=datagrouped["Date"].apply(lambda row:get_count_days(row))
    
    
    # datagrouped["CostUnitaire"]=datagrouped["CostTot"]/datagrouped["Instances"]
    datagrouped["UnitCostReservation1Y"]=datagrouped["CostReservation1Y"]/datagrouped["UsageReservation1Y"]
    datagrouped["UnitCostReservation1Y"]=datagrouped["UnitCostReservation1Y"].fillna(0)
    # return datagrouped

    datagrouped["CostJ"]=datagrouped["CostTot"]/datagrouped["NombreJoursMois"]
    datagrouped["CostJI"]=datagrouped["CostJ"]/datagrouped["Instances"]

    # return datagrouped

    datagrouped["UsageJ"]=datagrouped["UsageTot"]/datagrouped["NombreJoursMois"]
    # datagrouped["VariationJI"]=0
    datagrouped["VariationJ"]=0.
    datagrouped["VariationJI"]=0.
    datagrouped["VariationTot"]=0.
    
    datagrouped["GainFonctionalitesSkuJI"]=0.
    datagrouped["GainChangementSkuJI"]=0.
    datagrouped["GainReservation1YJI"]=0.
    datagrouped["GainContratsJI"]=0.

    datagrouped["GainTotDecom"]=0.
    datagrouped["GainTotSku"]=0.
    datagrouped["GainTotSizing"]=0.
    datagrouped["GainTotReservation"]=0.
    datagrouped["GainTotContrats"]=0.
    datagrouped["GainTotOptims"]=0.
    # datagrouped["GainTotDecom"]=0.


    # datagrouped["GainTarifJ"]=0.
    
    datagrouped["GainSizingJ"]=0.
    datagrouped["GainJ"]=0.
    datagrouped["GainDecommissionnementJ"]=0.
    datagrouped["GainJI"]=0.
    datagrouped["GainTot"]=0.
    
    datagrouped= datagrouped.sort_values(["ResourceName","ResourcePath","ResourceGroupName","Config","Date"])
    # datadecom = datagrouped.copy().iloc[0:0]
    # return datagrouped
    
    index_instance=0
    # with warnings.catch_warnings():
    #     warnings.simplefilter(action='ignore', category=pd.core.common.SettingWithCopyWarning)
        
    while index_instance<len(datagrouped)-1:
        row_reference=datagrouped.iloc[index_instance]
        gainJ_reference=0
        i=index_instance+0

        row_parcourue=datagrouped.iloc[i].copy()
        # if not row_parcourue["ResourceGroupName"]==datagrouped.loc[i,"ResourceGroupName"]:
        #     print(i,row_parcourue["Date"],row_parcourue["ResourceGroupName"],row_parcourue["ResourceName"],datagrouped.loc[i,"ResourceGroupName"])
            
        while i<len(datagrouped) and row_reference["Date"]==date_min and row_parcourue["ResourcePath"]==row_reference["ResourcePath"] and row_parcourue["ResourceGroupName"]==row_reference["ResourceGroupName"]:
            row_parcourue=datagrouped.iloc[i].copy()
                        
            # print(i,row_parcourue["Date"],row_parcourue["ResourceGroupName"],row_parcourue["ResourceName"])
            # print(i-1,row_reference["Date"],)
            if row_parcourue["UsageTot"]>0 and row_parcourue["ResourceName"]==row_reference["ResourceName"] and row_parcourue["ResourceGroupName"]==row_reference["ResourceGroupName"]:
                
                #Gain fonctionalités
                if row_reference["UnitCostOnDemand"]>0  and row_parcourue["UnitCostOnDemand"]>0 and sorted(row_reference["ResourceSku"])==sorted(row_parcourue["ResourceSku"]):
                    datagrouped["GainFonctionalitesSkuJI"].iloc[i]=24*(row_parcourue["UnitCostOnDemand"]-row_reference["UnitCostOnDemand"]) #*row_parcourue["UsageTot"]/(row_parcourue["NombreJoursMois"])

                
                #Gain changement SKU
                if row_reference["UnitCostOnDemand"]>0  and sorted(row_reference["ResourceSku"])!=sorted(row_parcourue["ResourceSku"]): #pas changement nb instances, pas changement sku, pas réservation
                    val1 = float(row_parcourue["UnitCostOnDemand"].copy())
                    val2=float(row_reference["UnitCostOnDemand"].copy())
                    datagrouped["GainChangementSkuJI"].iloc[i]=24*(val1-val2)
                    # print(i,24*(row_parcourue["UnitCostOnDemand"]-row_reference["UnitCostOnDemand"]),datagrouped.at[i,"GainChangementSkuJI"])
                #Gain Sizing
                if row_reference["UnitCostOnDemand"]>0 : #pas changement nb instances, pas changement sku, pas contrat
                    datagrouped["GainSizingJ"].iloc[i]=24*(row_parcourue["Instances"]-row_reference["Instances"])*row_parcourue["CostTot"]/row_parcourue["UsageTot"]
                    
                #Gain Reservation
                if row_parcourue["UsageReservation1Y"]>0: #pas changement nb instances, pas changement sku, pas contrat
                    # print(row_parcourue["UnitCostReservation1Y"],row_reference["UnitCostOnDemand"],row_parcourue["UsageReservation1Y"],row_parcourue["Instances"]   )
                    # row_precedente=datagrouped.iloc[i-1]
                    datagrouped["GainReservation1YJI"].iloc[i]=24*(row_parcourue["UnitCostReservation1Y"]-row_parcourue["UnitCostOnDemandNegocie"])*row_parcourue["UsageReservation1Y"]/row_parcourue["UsageTot"]
                
                #Gain Contrats
                if row_parcourue["UnitCostOnDemand"]!=row_parcourue["UnitCostOnDemandNegocie"] and gain_contrat:
                    datagrouped["GainContratsJI"].iloc[i]=24*(row_parcourue["UnitCostOnDemandNegocie"]-row_parcourue["UnitCostOnDemand"])
                
                # datagrouped["VariationTot"].iloc[i]=row_parcourue["CostTot"]-row_reference["CostTot"]
                datagrouped["CostJI"].iloc[i]=datagrouped["CostJ"].iloc[i]/datagrouped["Instances"].iloc[i]
                datagrouped["VariationJ"].iloc[i]= datagrouped["CostJ"].iloc[i]-row_reference["CostJ"]
                datagrouped["VariationJI"].iloc[i]= datagrouped["CostJI"].iloc[i]-row_reference["CostJI"]

                datagrouped["VariationTot"].iloc[i]= (datagrouped["CostJ"].iloc[i]-row_reference["CostJ"])*datagrouped["NombreJoursMois"].iloc[i]
                
                datagrouped["GainJI"].iloc[i]=datagrouped["GainChangementSkuJI"].iloc[i]+datagrouped["GainReservation1YJI"].iloc[i]+datagrouped["GainContratsJI"].iloc[i]+datagrouped["GainFonctionalitesSkuJI"].iloc[i]
                
                if i==index_instance and not gains_premier_mois:
                    gainJ_reference=(datagrouped["GainJI"].iloc[i])*row_reference["Instances"]+datagrouped["GainSizingJ"].iloc[i]

                datagrouped["GainJ"].iloc[i]=datagrouped["GainJI"].iloc[i]*row_reference["Instances"]+datagrouped["GainSizingJ"].iloc[i]-gainJ_reference
                # print(gainJ_reference)
                # datagrouped["GainJ"].iloc[i]=(datagrouped["GainReservation1YJI"].iloc[i]+datagrouped["GainChangementSkuJI"].iloc[i]+datagrouped["GainContratsJI"].iloc[i])*row_reference["Instances"]+datagrouped["GainSizingJ"].iloc[i]
                datagrouped["CostJI"].iloc[i]=datagrouped["CostJ"].iloc[i]/datagrouped["Instances"].iloc[i]
                # datagrouped["VariationTot"].iloc[i]=row_parcourue["CostTot"]-row_reference["CostTot"]
                
                
                
                datagrouped["GainTotSku"].iloc[i]=(datagrouped["GainChangementSkuJI"].iloc[i]+datagrouped["GainFonctionalitesSkuJI"].iloc[i])*row_reference["Instances"]*datagrouped["NombreJoursMois"].iloc[i]
                datagrouped["GainTotSizing"].iloc[i]=datagrouped["GainSizingJ"].iloc[i]*datagrouped["NombreJoursMois"].iloc[i]
                datagrouped["GainTotReservation"].iloc[i]=datagrouped["GainReservation1YJI"].iloc[i]*row_reference["Instances"]*datagrouped["NombreJoursMois"].iloc[i]
                datagrouped["GainTotContrats"].iloc[i]=datagrouped["GainContratsJI"].iloc[i]*row_reference["Instances"]*datagrouped["NombreJoursMois"].iloc[i]

                # +datagrouped["GainContratsJI"].iloc[i]
                # datagrouped["GainTotOptims"].

                #Gain Décommissionnement
                
                if row_parcourue["Date"]<date_max:# and False:
                    if i<len(datagrouped)-1:
                        row_suivante=datagrouped.iloc[i+1].copy()
                        
                        
                    #déclencher le décom
                    
                    if ( i<len(datagrouped)-1 and (row_suivante["ResourceName"]!=row_parcourue["ResourceName"] or row_suivante["ResourceGroupName"]!=row_parcourue["ResourceGroupName"] )) or i==len(datagrouped)-1:
                        
                        # Add one month to the date using relativedelta
                        last_date = datetime.strptime(row_parcourue["Date"], "%Y-%m-%d")
                        row_decom=datagrouped.iloc[i].copy()
                        # print(row_decom["GainSizingJ"])
                        
                        row_decom["UsageTot"]=0.
                        row_decom["CostTot"]=0.
                        row_decom["UsageOnDemand"]=0.
                        row_decom["CostOnDemand"]=0.
                        row_decom["UsageReservation1Y"]=0.
                        row_decom["CostReservation1Y"]=0.
                        row_decom["InstancesBrut"]=0.
                        row_decom["CostJI"]=0.
                        row_decom["UsageJ"]=0.
                        row_decom["Instances"]=0.
                        row_decom["VariationJ"]=-row_reference["CostTot"].copy()/row_reference["NombreJoursMois"].copy()
                        # row_decom["VariationJI"]=-row_reference["CostTotJI"]
                        
                        row_decom["GainDecommissionnementJ"]=-row_parcourue["CostJ"].copy()
                        row_decom["GainJ"]=row_decom["GainJ"]+row_decom["GainDecommissionnementJ"]
                        row_decom["CostJ"]=0.


                        while last_date.strftime("%Y-%m-%d")<date_max: # ajout lignes jusqu'à date max
                            
                            new_date = (last_date + relativedelta(months=1))
                            
                            row_decom["Date"]=new_date.strftime("%Y-%m-%d")
                            row_decom["NombreJoursMois"]=get_count_days(new_date.strftime("%Y-%m-%d"))

                            row_decom["VariationTot"]=-row_reference["CostTot"].copy()/row_reference["NombreJoursMois"]*row_decom["NombreJoursMois"]
                            row_decom["GainTot"]=row_decom["GainJ"]*row_decom["NombreJoursMois"]



                            row_decom["GainTotSku"]=(row_decom["GainChangementSkuJI"]+row_decom["GainFonctionalitesSkuJI"])*row_reference["Instances"]*row_decom["NombreJoursMois"]
                            row_decom["GainTotSizing"]=row_decom["GainSizingJ"]*row_decom["NombreJoursMois"]
                            row_decom["GainTotReservation"]=row_decom["GainReservation1YJI"]*row_reference["Instances"]*row_decom["NombreJoursMois"]
                            row_decom["GainTotContrats"]=row_decom["GainContratsJI"]*row_reference["Instances"]*row_decom["NombreJoursMois"]
                            row_decom["GainTotDecom"]=row_decom["GainDecommissionnementJ"]*row_decom["NombreJoursMois"]
                                                                
                            last_date = new_date

                            datagrouped = pd.concat([datagrouped, pd.DataFrame([row_decom]).reset_index(drop=True)], ignore_index=True, axis=0).reset_index(drop=True)

                            i+=1
                        datagrouped=datagrouped.sort_values(["ResourceName","ResourcePath","ResourceGroupName","Config","Date"])

                row_parcourue=datagrouped.iloc[i].copy()
            i+=1
            
        index_instance+=1

    
    datagrouped["GainTot"]= datagrouped["GainJ"]*datagrouped["NombreJoursMois"]
    datagrouped["GainTotOptims"]=datagrouped["GainTotDecom"]+datagrouped["GainTotSku"]+datagrouped["GainTotContrats"]+datagrouped["GainTotSizing"]+datagrouped["GainTotReservation"]
    datagrouped["Ecart"]=datagrouped["VariationTot"]-datagrouped["GainTot"]
    # test
    datagrouped["GainJI"]=datagrouped["GainChangementSkuJI"]+datagrouped["GainReservation1YJI"]+datagrouped["GainContratsJI"]+datagrouped["GainFonctionalitesSkuJI"]

    return datagrouped
   
donnees = resourcesCoreModel.copy()
date_min="2022-10-01"

resourceName=None
gains_tot_virtual_machines_CoreModel=get_gains_totaux_virtual_machines(donnees,date_min,gain_contrat=True,gains_premier_mois=True,resourceName=resourceName)

gains_tot_virtual_machines_CoreModel[(gains_tot_virtual_machines_CoreModel["GainTot"]-gains_tot_virtual_machines_CoreModel["VariationTot"])/gains_tot_virtual_machines_CoreModel["GainTot"]>0.1].to_csv("C:\\Users\\hugo.dufrene\\Wavestone\\WE - ROLEX [CH] - Support mise en place Gouvernance Cloud public - 13665 - Documents partages\\Gouvernance Cloud\\FinOps\\Optimisations\\Virtual Machines\\20240112 - Analyse Gains Optims Virtual Machines.csv",index=False)
%store gains_tot_virtual_machines_CoreModel

gains_tot_virtual_machines_CoreModel.to_csv("C:\\Users\\hugo.dufrene\\Wavestone\\WE - ROLEX [CH] - Support mise en place Gouvernance Cloud public - 13665 - Documents partages\\Gouvernance Cloud\\FinOps\\Optimisations\\Virtual Machines\\20240112 - Analyse Gains Optims Virtual Machines.csv",index=False)
plot_gains_tot(gains_tot_virtual_machines_CoreModel.copy(),services=["Virtual Machines"],date_min="2022-10-01")

# gains_tot_virtual_machines_CoreModel[["Date","ResourceName","Instances","ResourceSku","UsageTot","UsageOnDemand","CostOnDemand","CostReservation1Y","UnitCostOnDemand","UnitCostOnDemandNegocie","CostJ","VariationJ","GainJ","CostJI","VariationJI","GainJI","GainFonctionalitesSkuJI","GainChangementSkuJI","GainReservation1YJI","GainContratsJI","GainSizingJ","GainDecommissionnementJ","VariationTot","GainTot" ]].sort_values("Date")


# %%
# gains_tot_virtual_machines_CoreModel=get_gains_totaux_virtual_machines(resourcesCoreModel.copy(),date_min,gain_contrat=False)
plot_gains_tot(gains_tot_virtual_machines_CoreModel.copy(),services=["Virtual Machines"],date_min="2022-10-01")
#Sans contrat


# %%
gains_tot_app_service_Core_Model[gains_tot_app_service_Core_Model["GainTotSku"]>0]

# %%
plot_gains_tot(gains_tot_app_service_Core_Model.copy(),services=["Azure App Service"],date_min="2022-10-01")


# %%
resourcesCoreModel.groupby("Date").sum()

# %%
def plot_gains_tot(donnees,services=None,cumule=False,date_min="2020-01-01"):
    locale.setlocale(locale.LC_ALL, '')
    
    colors=["FF0000","003AFF","23FF00","FFB900","00F3FF","FF0093","00FFB9","8B00FF","FFFF00","FF0097","B6FF00"]
    
    donnees=donnees.rename(columns={"ServiceName":"Service"})
    donnees=donnees[donnees["Date"]>=date_min]
    
    # Convertir la colonne "date" en format de date
    dates=donnees["Date"].unique()
    donnees['Date'] = pd.to_datetime(donnees['Date'])
    
    # Trier le DataFrame par ordre croissant de date
    donnees = donnees.sort_values('Date')

    dates=list(dates)
    dates.sort()
    # Créer une liste de traces pour chaque service
    traces = []

    df_grouped=donnees.groupby(['Date',"Service"])[["CostTot","GainTot","GainTotDecom","GainTotSku","GainTotSizing","GainTotReservation","GainTotContrats","GainTotOptims"]].sum().reset_index()
    # df_grouped["GainTot"]=-df_grouped["GainTot"]
    # df_grouped["GainTot"]=df_grouped.apply(lambda row : donnees[(donnees["Date"]==row["Date"])&(donnees["Service"]==row["Service"])]["GainTot"].sum(),axis=1)
  

    if services==None:
        services = df_grouped["Service"].unique()

    
    for index_service in range(len(services)):
        
        service=services[index_service]
        print(service)
        base_color = colors[index_service+3]
        dark_color, light_color = generate_color_shades(base_color)
        

        
        data=df_grouped[df_grouped["Service"]==service]
        

        if not data.empty:
        
            data["Date"]=pd.to_datetime(data["Date"])
            data=data.sort_values("Date")
            
            data["GainTot"]=-data["GainTot"]
            data["GainTotDecom"]=-data["GainTotDecom"]
            data["GainTotSku"]=-data["GainTotSku"]
            data["GainTotSizing"]=-data["GainTotSizing"]
            data["GainTotReservation"]=-data["GainTotReservation"]
            data["GainTotContrats"]=-data["GainTotContrats"]
            data["GainTotOptims"]=-data["GainTotOptims"]   

            data["GainTotCumule"]=data.apply(lambda row:data[(data["Date"]<=row["Date"])]["GainTot"].sum(),axis=1)
            data["GainTotDecomCumule"]=data.apply(lambda row:data[(data["Date"]<=row["Date"])]["GainTotDecom"].sum(),axis=1)
            data["GainTotSkuCumule"]=data.apply(lambda row:data[(data["Date"]<=row["Date"])]["GainTotSku"].sum(),axis=1)
            data["GainTotSizingCumule"]=data.apply(lambda row:data[(data["Date"]<=row["Date"])]["GainTotSizing"].sum(),axis=1)

            data["GainTotReservationCumule"]=data.apply(lambda row:data[(data["Date"]<=row["Date"])]["GainTotReservation"].sum(),axis=1)  
            data["GainTotContratsCumule"]=data.apply(lambda row:data[(data["Date"]<=row["Date"])]["GainTotContrats"].sum(),axis=1)  




            data["CostTotSansGainTotCumule"]=data["CostTot"]+data["GainTotCumule"]
            data["CostTotSansGainTot"]=data["CostTot"]+data["GainTot"]
            data["Nombre Jours"]=data["Date"].apply(lambda row : get_count_days(row.strftime("%Y-%m-%d")))
            # print(data[data["GainRightSizing"]>0]["GainRightSizing"].sum())
            # return data
            # return data
            # return data
            # print(data["GainCumule"].max())
           


            show_data=pd.concat([pd.DataFrame.from_dict([{
                    "Date": (data["Date"].iloc[0]- timedelta(days=28)).replace(day=1).replace(hour=0).replace(minute=0).replace(second=0).replace(microsecond=0),
                    "Service":service,
                    "CostTot":0,
                    "GainTot":0,
                    "GainTotCumule":0,
                    "CostTotSansGainTot":0,
                    "CostTotSansGainTotCumule":0            
                }]),data.copy()], ignore_index=True)
            show_data=data.copy()
            
            if cumule:
                trace = go.Scatter(
                    x=show_data["Date"],  # Concaténation des coordonnées x dans l'ordre croissant et décroissant
                    y=show_data["CostTotSansGainTotCumule"],  # Ajout de zéros à la fin pour fermer l'aire sous la courbe
                    # fill='tozeroy',  # Remplit l'aire sous la courbe jusqu'à l'axe y=0
                    mode='lines',  # Mode "lines" pour obtenir une ligne continue
                    # fillcolor='rgba(0, 176, 246, 0.2)',  # Couleur de remplissage de l'aire sous la courbe
                    name=service+" projeté",
                    hovertemplate="%{y:,.0f} kCHF",
                    line={"color":light_color}
                )
                traces.append(trace)
            else:
                trace = go.Scatter(
                    x=show_data["Date"],  # Concaténation des coordonnées x dans l'ordre croissant et décroissant
                    y=show_data["CostTotSansGainTot"],  # Ajout de zéros à la fin pour fermer l'aire sous la courbe
                    # fill='tozeroy',  # Remplit l'aire sous la courbe jusqu'à l'axe y=0
                    mode='lines',  # Mode "lines" pour obtenir une ligne continue
                    # fillcolor='rgba(0, 176, 246, 0.2)',  # Couleur de remplissage de l'aire sous la courbe
                    name=service+" projeté",
                    hovertemplate="%{y:,.0f} kCHF",
                    line={"color":"#A37E2C"}
                )
                traces.append(trace)
                
            #Coûts totaux
            trace = go.Scatter(
                x=show_data['Date'],  # Concaténation des coordonnées x dans l'ordre croissant et décroissant
                y=show_data["CostTot"],  # Ajout de zéros à la fin pour fermer l'aire sous la courbe
                # fill='tozeroy',  # Remplit l'aire sous la courbe jusqu'à l'axe y=0
                mode='lines',  # Mode "lines" pour obtenir une ligne continue
                # fillcolor='rgba(0, 176, 246, 0.2)',  # Couleur de remplissage de l'aire sous la courbe
                name=service,
                hovertemplate="%{y:,.0f} kCHF",
                # text=service  # Add the series name as the text for each point
                line={"color":"#006039"}
            )

            traces.append(trace)
            
            bars = go.Bar(
            x=df_grouped['Date'],
            y=-(df_grouped["GainTot"]),
            # text=round(data['CostTot'], 1),
            hovertemplate='%{y:,.0f}<extra></extra>',  # Modèle de survol avec une seule décimale
             yaxis="y2",
            # width= 5,  # Adjust this value to control spacing between bar groups
            name="Gains",
            marker_color="#A37E2C"
            )
            traces.append(bars)

     # Définir le layout du graphique
    layout = go.Layout(
        barmode='stack',  # Barres empilées
        xaxis=dict(title='Date'),
        yaxis=dict(title="Coût"),
        title="Estimation des gains tot "+" ".join(services),
        legend=dict(x=1.05, y=1, bgcolor='rgba(255, 255, 255, 0.5)'),
        margin=dict(l=75, r=50, t=80, b=80),
        showlegend=True
    )
    max_y = max(show_data["CostTot"].max(),0)
            
    max_y = max(max_y,show_data["CostTotSansGainTot"].max())

    # Créer la figure
    fig = go.Figure(data=traces, layout=layout)
    fig.update_layout(
        xaxis_range=[min(df_grouped["Date"]),max(df_grouped["Date"])],
        yaxis=dict(title='Coûts réels / Coûts projetés',rangemode='nonnegative'),
        yaxis2=dict(title='Gains tot', overlaying='y', side='right',showgrid=False),
        legend=dict(x=1.05, y=1, bgcolor='rgba(255, 255, 255, 0.5)'),
        yaxis_range=[0, math.ceil(max_y/10000)*10000],
        yaxis2_range=[0,math.ceil(max_y/10000)*10000],
        bargap=0.5
        
    )
    fig.update_xaxes(
        tickmode='array',
        tickvals=df_grouped["Date"],
        tickangle=270,  # Angle de rotation des ticks
        tickformat='%m.%y',  # Format des ticks de date (optionnel)
    )
    # print(math.ceil(max(show_data["CostTotSansGainTot"])/10000)*10000)
    # Afficher le graphique
    fig.show()
    
    data=data.rename(columns={"Service":"ServiceName"})
    # data["CostJ"]=data["CostTot"]/data["Nombre Jours"]
    # data["VariationJ"]=data["CostJ"]-data["CostJ"].iloc[0]
    return data[["Date",	"ServiceName",	"CostTot",	"CostTotSansGainTot", "GainTot","GainTotCumule",	"GainTotDecom","GainTotDecomCumule",	"GainTotSku","GainTotSkuCumule","GainTotSizing","GainTotSizingCumule","GainTotReservation","GainTotReservationCumule",		"GainTotContrats","GainTotContratsCumule",						"CostTotSansGainTotCumule"	]]

plot_gains_tot(filtered_df.copy(),services=["Azure App Service"],date_min="2022-10-01")



# %% [markdown]
# gains_tot_sql_database_CoreModel[(gains_tot_sql_database_CoreModel["Date"]<"2023-09-01")&(abs((gains_tot_sql_database_CoreModel["GainTot"]-gains_tot_sql_database_CoreModel["VariationTot"])/gains_tot_sql_database_CoreModel["VariationTot"])>0.01)&(gains_tot_sql_database_CoreModel["GainDecommissionnementJ"]==0)]

# %%
gains_tot_sql_database_US=gains_tot_sql_database_CoreModel[gains_tot_sql_database_CoreModel["ProjectName"]=="IT-PROD-US"].copy()
gains_tot_app_service_US=gains_tot_app_service_Core_Model[gains_tot_app_service_Core_Model["ProjectName"]=="IT-PROD-US"].copy()
gains_tot_virtual_machines_US=gains_tot_virtual_machines_CoreModel[gains_tot_virtual_machines_CoreModel["ProjectName"]=="IT-PROD-US"].copy()

# %%
plot_gains_tot(gains_tot_sql_database_US.copy(),services=["SQL Database"],date_min="2023-05-01")

# %%
plot_gains_tot(gains_tot_app_service_US.copy(),services=["Azure App Service"],date_min="2023-05-01")

# %%
plot_gains_tot(gains_tot_virtual_machines_US.copy(),services=["Virtual Machines"],date_min="2023-05-01")

# %%
gains_tot_sql_database_CoreModel[(gains_tot_sql_database_CoreModel["Date"]=="2023-12-01")&(gains_tot_sql_database_CoreModel["Ecart"]>1)].sort_values("Ecart",ascending=False).to_csv("C:\\Users\\hugo.dufrene\\Wavestone\\WE - ROLEX [CH] - Support mise en place Gouvernance Cloud public - 13665 - Documents partages\\Gouvernance Cloud\\FinOps\\Optimisations\\SQL Database\\20240122 - Analyse Gains Optims SQL Database TEST.csv",index=False)

# %%
resourcesCoreModel[resourcesCoreModel["ResourceName"]=="sqldb-gr-bi-test10-byod"][["Date","ResourceName","ResourceSkuSize","UsageTot","CostTot"]].sort_values("Date")

# gains_tot_sql_database_CoreModel[gains_tot_sql_database_CoreModel["ResourceName"]=="sql-gr-bi-test10"][["Date","ResourceName","ResourceSku","UsageTot","CostTot","GainTot"]]

# %%
donnees = resourcesCoreModel.copy()
date_min="2022-10-01"
# lignesDecommissionnement=get_decommissionnement(resourcesAzure)
# plot_cost_decom(donnees.copy(),gains_decommissionnement.copy(),services=["SQL Database"],date_min=date_min)
plot_cost_decom(donnees.copy(),gains_decommissionnement_CoreModel.copy(),service="Azure App Service"],date_min=date_min)


# %%
plot_gains_tot(gains_tot_sql_database_CoreModel.copy(),services=["SQL Database"],date_min="2022-10-01")
#Sans contrat

# %%
plot_gains_tot(gains_tot_sql_database_CoreModel.copy(),services=["SQL Database"],date_min="2022-10-01")
#Avec contrat

# %%
resourcesCoreModel[(resourcesCoreModel["ServiceName"]=="SQL Database")&(resourcesCoreModel["Date"]=="2022-10-01")]["CostTot"].sum()

# %%
gains_tot_sql_database_CoreModel[["Date","Config","Instances","ResourceName","ResourceGroupName","ResourceSku","UsageOnDemand","CostOnDemand","UnitCostOnDemand","UnitCostOnDemandNegocie","UsageReservation1Y","CostReservation1Y","UnitCostReservation1Y","CostJ","VariationJ","GainJ","CostJI","GainJI","GainChangementSkuJI","GainReservation1YJI","GainContratsJI","GainSizingJ","VariationTot","GainTot"]]


# %%
def plot_cost_sizing(donnees,services=None,cumule=False,date_min="2020-01-01"):
    locale.setlocale(locale.LC_ALL, '')
    
    colors=["FF0000","003AFF","23FF00","FFB900","00F3FF","FF0093","00FFB9","8B00FF","FFFF00","FF0097","B6FF00"]
    
    donnees=donnees.rename(columns={"ServiceName":"Service"})
    donnees=donnees[donnees["Date"]>=date_min]
    
    # Convertir la colonne "date" en format de date
    dates=donnees["Date"].unique()
    donnees['Date'] = pd.to_datetime(donnees['Date'])
    
    # Trier le DataFrame par ordre croissant de date
    donnees = donnees.sort_values('Date')

    dates=list(dates)
    dates.sort()
    # Créer une liste de traces pour chaque service
    traces = []

    df_grouped=donnees.groupby(['Date',"Service"])[["CostTot","GainSizing"]].sum().reset_index()
    df_grouped["GainSizing"]=-df_grouped["GainSizing"]
    df_grouped["GainSizing"]=df_grouped.apply(lambda row : donnees[(donnees["Date"]==row["Date"])&(donnees["GainSizing"]!=0)&(donnees["Service"]==row["Service"])]["GainSizing"].sum(),axis=1)
  
    if services==None:
        services = df_grouped["Service"].unique()

    max_y = 0
    for index_service in range(len(services)):
        
        service=services[index_service]
        print(service)
        base_color = colors[index_service+3]
        dark_color, light_color = generate_color_shades(base_color)
        

        
        data=df_grouped[df_grouped["Service"]==service]
   
        if not data.empty:
        
            data["Date"]=pd.to_datetime(data["Date"])
            data=data.sort_values("Date")
            
            data["GainSizing"]=-data["GainSizing"]
            data["GainSizingCumule"]=data.apply(lambda row:data[(data["Date"]<=row["Date"])]["GainSizing"].sum(),axis=1)
            data["CostTotSansGainSizingCumule"]=data["CostTot"]+data["GainSizingCumule"]
            data["CostTotSansGainSizing"]=data["CostTot"]+data["GainSizing"]

            # print(data[data["GainRightSizing"]>0]["GainRightSizing"].sum())
            # return data
            # return data
            # return data
            # print(data["GainCumule"].max())
            max_y = max(max_y,data["GainSizingCumule"].max())


            show_data=pd.concat([pd.DataFrame.from_dict([{
                    "Date": (data["Date"].iloc[0]- timedelta(days=30)).replace(hour=0).replace(minute=0).replace(second=0).replace(microsecond=0),
                    "Service":service,
                    "CostTot":0,
                    "GainSizing":0,
                    "GainSizingCumule":0,
                    "CostTotSansGainSizing":0,
                    "CostTotSansGainSizingCumule":0            
                }]),data.copy()], ignore_index=True)
            show_data=data.copy()
            
            if cumule:
                trace = go.Scatter(
                    x=show_data["Date"],  # Concaténation des coordonnées x dans l'ordre croissant et décroissant
                    y=show_data["CostTotSansGainSizingCumule"],  # Ajout de zéros à la fin pour fermer l'aire sous la courbe
                    # fill='tozeroy',  # Remplit l'aire sous la courbe jusqu'à l'axe y=0
                    mode='lines',  # Mode "lines" pour obtenir une ligne continue
                    # fillcolor='rgba(0, 176, 246, 0.2)',  # Couleur de remplissage de l'aire sous la courbe
                    name=service+" projeté",
                    hovertemplate="%{y:,.0f} kCHF",
                    line={"color":light_color}
                )
                traces.append(trace)
            else:
                trace = go.Scatter(
                    x=show_data["Date"],  # Concaténation des coordonnées x dans l'ordre croissant et décroissant
                    y=show_data["CostTotSansGainSizing"],  # Ajout de zéros à la fin pour fermer l'aire sous la courbe
                    # fill='tozeroy',  # Remplit l'aire sous la courbe jusqu'à l'axe y=0
                    mode='lines',  # Mode "lines" pour obtenir une ligne continue
                    # fillcolor='rgba(0, 176, 246, 0.2)',  # Couleur de remplissage de l'aire sous la courbe
                    name=service+" projeté",
                    hovertemplate="%{y:,.0f} kCHF",
                    line={"color":"#A37E2C"}
                )
                traces.append(trace)
                
            #Coûts totaux
            trace = go.Scatter(
                x=show_data['Date'],  # Concaténation des coordonnées x dans l'ordre croissant et décroissant
                y=show_data["CostTot"],  # Ajout de zéros à la fin pour fermer l'aire sous la courbe
                # fill='tozeroy',  # Remplit l'aire sous la courbe jusqu'à l'axe y=0
                mode='lines',  # Mode "lines" pour obtenir une ligne continue
                # fillcolor='rgba(0, 176, 246, 0.2)',  # Couleur de remplissage de l'aire sous la courbe
                name=service,
                hovertemplate="%{y:,.0f} kCHF",
                # text=service  # Add the series name as the text for each point
                line={"color":"#006039"}
            )

            traces.append(trace)
            
            bars = go.Bar(
            x=df_grouped['Date'],
            y=(df_grouped["GainSizing"]),
            # text=round(data['CostTot'], 1),
            hovertemplate='%{y:,.0f}<extra></extra>',  # Modèle de survol avec une seule décimale
             yaxis="y2",
            # width= 5,  # Adjust this value to control spacing between bar groups
            name="Gains",
            marker_color="#A37E2C"
            )
            traces.append(bars)

     # Définir le layout du graphique
    layout = go.Layout(
        barmode='stack',  # Barres empilées
        xaxis=dict(title='Date'),
        yaxis=dict(title="Coût"),
        title="Estimation des gains générés par le changement de nombre d'instances",
        legend=dict(x=1.05, y=1, bgcolor='rgba(255, 255, 255, 0.5)'),
        margin=dict(l=75, r=50, t=80, b=80),
        showlegend=True
    )
    
    # Créer la figure
    fig = go.Figure(data=traces, layout=layout)
    fig.update_layout(
        xaxis_range=[min(df_grouped["Date"]),max(df_grouped["Date"])],
        yaxis=dict(title='Coûts réels / Coûts projetés',rangemode='nonnegative'),
        yaxis2=dict(title='Gains sizing', overlaying='y', side='right',rangemode='nonnegative',showgrid=False),
        legend=dict(x=1.05, y=1, bgcolor='rgba(255, 255, 255, 0.5)'),
        # yaxis_range=[10000, 200000],
        # yaxis2_range=[0,60000],
        bargap=0.5
        
    )
    fig.update_xaxes(
        tickmode='array',
        tickvals=df_grouped["Date"],
        tickangle=270,  # Angle de rotation des ticks
        tickformat='%m.%y',  # Format des ticks de date (optionnel)
    )
    
    # Afficher le graphique
    fig.show()
    
    data=data.rename(columns={"Service":"ServiceName"})

    return data
plot_cost_sizing(gains_sizing_app_service.copy(),services=["Azure App Service"],date_min="2022-10-01")
# gains_sizing_app_service[(gains_sizing_app_service["Date"]=="2022-11-01")&(gains_sizing_app_service["GainSizing"]!=0)]["GainSizing"].sum()

# %%
def get_gains_rightsizing(donnees,service,date_min):
    # print("TEST")
    donnees=donnees[donnees["ServiceName"]==service]
    
    # gains_rightsizing_app_service[gains_rightsizing_app_service["ResourceName"]=="asp-au-cmss-prod01"]
    # donnees=donnees[donnees["ResourceName"]=="asp-au-cmss-prod01"]
    # print("TEST")
    # donnees=donnees[(donnees["ApplicationName"]=="Core Model (WSA)")&(donnees["Date"]>"2022-01-01")]
    # donnees=donnees[donnees["ProjectName"]=="IT-DEV"]
    # print("TEST")

    data_source = donnees.groupby(["Date","ProjectName","ServiceName","ResourceName"])[["CostTot","UsageTot","CostOnDemand","UsageOnDemand"]].sum().reset_index()
    data_source["UnitCostOnDemand"]=0.0
    data_source["UnitCostOnDemandNegocie"]=0.0
    # return data_source
    data=donnees.groupby(["Date","ResourceName","ProjectName","ApplicationName","LocationName","ServiceName","ResourceGroupName","ResourcePath","ServiceTier","ResourceSkuSize"])[["UsageTot","UsageOnDemand","CostTot","CostOnDemand"]].sum().reset_index()
   
    
    data=data[data["Date"]>=date_min]
    
    data= data.sort_values(["ResourceName","ResourcePath","Date"])
  
    gains=None
 
    data = data.fillna(0)
    data["Config"] = data.apply(lambda row: get_config(row), axis=1)
    
    if service=="Azure App Service":
        datagrouped=data.groupby(["Date","ResourceName","ProjectName","ApplicationName","LocationName","ServiceName","ResourceGroupName","ResourcePath","Config"])[["UsageTot","CostTot","UsageOnDemand","CostOnDemand"]].sum().reset_index()
        datagrouped["ResourceSkuSize"] = datagrouped.apply(lambda row: list(data[
        (data["Date"] == row["Date"]) &
        (data["ResourceName"] == row["ResourceName"]) &
        (data["Config"] ==row["Config"]   )    ]["ResourceSkuSize"].unique()), axis=1)

    if service=="SQL Database":
        datagrouped=data.groupby(["Date","ResourceName","ProjectName","ApplicationName","LocationName","ServiceName","ResourceGroupName","ResourcePath"])[["UsageTot","CostTot","UsageOnDemand","CostOnDemand"]].sum().reset_index()

        datagrouped["ResourceSkuSize"] = datagrouped.apply(lambda row :   list(data[        (data["Date"] == row["Date"]) &        (data["ResourceName"] == row["ResourceName"])&(data ["ResourcePath"]==row["ResourcePath"]) ]["ResourceSkuSize"].unique()), axis=1)
      
    datagrouped["UnitCostOnDemand"]=0
    datagrouped["UnitCostOnDemandNegocie"]=0
    # datagrouped[["UnitCostOnDemand","FacteurRabaisNegociationContrat"]]=datagrouped.apply(lambda row : get_unit_cost_on_demand_v2(data_source,row,service,negocie=False),axis=1)
    datagrouped[["UnitCostOnDemand", "FacteurRabaisNegociationContrat"]] = datagrouped.apply(
        lambda row: pd.Series(get_unit_cost_on_demand_v2(data_source, row, service, negocie=False)),
        axis=1
    )
    datagrouped["UnitCostOnDemandNegocie"]=datagrouped["UnitCostOnDemand"]*datagrouped["FacteurRabaisNegociationContrat"]
    # return datagrouped[datagrouped["ResourceName"]=="asp-au-cmss-prod01"]
    # print("UnitCostOnDemand récupéré")
    
    # datagrouped["UnitCostOnDemandNegocie"]=datagrouped.apply(lambda row : get_unit_cost_on_demand(data_source,row,service,negocie=True),axis=1)
    # print("UnitCostOnDemandMNegocie récupéré")
    
    datagrouped=datagrouped.sort_values(["ResourcePath","Date"])
    # datagrouped["UnitCostOnDemand"]=datagrouped["CostOnDemand"]/datagrouped["UsageOnDemand"]
    datagrouped["AncienSku"]= "" #datagrouped.apply(lambda row: [], axis=1)
    datagrouped["NouveauSku"]= "" #datagrouped.apply(lambda row: [], axis=1)
    datagrouped["GainRightSizing"] = datagrouped.apply(lambda row: 0.0, axis=1)
    # datagrouped=datagrouped.fillna(0)
    
    # return datagrouped
    counter={}

    changes=pd.DataFrame([])
    print("Pré traitement ok")
    
    for i in range(len(datagrouped)-2):
        row1=datagrouped.iloc[i]
        row2=datagrouped.iloc[i+1]
        row3=datagrouped.iloc[i+2]
        
        # print(row1["Date"],row1["ResourceName"],row1["ResourceSkuSize"],"=> ",row2["Date"],row2["ResourceName"],row2["ResourceSkuSize"],"=> ",row3["Date"],row3["ResourceName"],row3["ResourceSkuSize"])


        if service=="Azure App Service":
            if row1["LocationName"]==row2["LocationName"] and row2["LocationName"]==row3["LocationName"] and row1["ResourceName"]==row2["ResourceName"] and row2["ResourceName"]==row3["ResourceName"] and row1["Config"]==row2["Config"] and row2["Config"]==row3["Config"] and (sorted(row1["ResourceSkuSize"])!=sorted(row2["ResourceSkuSize"]) or len(row1["ResourceSkuSize"])!=len(row2["ResourceSkuSize"])) and (sorted(row2["ResourceSkuSize"])!=sorted(row3["ResourceSkuSize"]) or len(row2["ResourceSkuSize"])!=len(row3["ResourceSkuSize"])):
                
                # if row1["ResourceName"]=="asp-es-cmss-prod01":
                
        
                print(row1["Date"],row1["ResourceName"],row1["ResourceSkuSize"],"=> ",row2["Date"],row2["ResourceName"],row2["ResourceSkuSize"],"=> ",row3["Date"],row3["ResourceName"],row3["ResourceSkuSize"])
                print(row1["Date"],row1["UnitCostOnDemand"],row1["UsageTot"],row1["CostTot"],"|",row3["Date"],row3["UnitCostOnDemand"],row3["UsageTot"],row3["CostTot"],"|",row3["UnitCostOnDemand"] - row1["UnitCostOnDemand"])
                
                datagrouped.iloc[i+1, datagrouped.columns.get_loc("AncienSku")] = ", ".join(row1["ResourceSkuSize"])
                datagrouped.iloc[i+1, datagrouped.columns.get_loc("NouveauSku")] =  ", ".join(row3["ResourceSkuSize"])
                
                # print("TEST")
                
                j=1
                while i+j<len(datagrouped["ResourceName"]):
                    if  datagrouped.iloc[i+j]["ResourcePath"]==row3["ResourcePath"] and datagrouped.iloc[i+j]["LocationName"]==row3["LocationName"]  and datagrouped.iloc[i+j]["ResourceName"]==row3["ResourceName"] and datagrouped.iloc[i+j]["Config"]==row3["Config"]:
                        
                        
                        datagrouped.iloc[i+j, datagrouped.columns.get_loc("GainRightSizing")] = (datagrouped.iloc[i+j]["UnitCostOnDemand"]-row1["UnitCostOnDemand"])*datagrouped.iloc[i+j]["UsageTot"] if datagrouped.iloc[i+j]["UsageTot"]>0 else row3["CostOnDemand"]-row1["CostOnDemand"]
                        # print(datagrouped.iloc[i+j]["Date"],datagrouped.iloc[i+j]["GainRightSizing"])
                        j+=1
                        # print(i,j,i+j,len(datagrouped))
                    else:
                        break
                

        if service=="SQL Database":
            if row1["ResourcePath"]==row2["ResourcePath"] and  row1["ResourcePath"]==row3["ResourcePath"] and row1["LocationName"]==row2["LocationName"] and row2["LocationName"]==row3["LocationName"] and row1["ResourceName"]==row2["ResourceName"] and row2["ResourceName"]==row3["ResourceName"]:
                
                skuSizes1=sorted([element for element in row1["ResourceSkuSize"] if "Data Stored" not in element])
                skuSizes2=sorted([element for element in row2["ResourceSkuSize"] if "Data Stored" not in element])
                skuSizes3 = sorted([element for element in row3["ResourceSkuSize"] if "Data Stored" not in element])
                if (skuSizes1!=skuSizes2 or len(skuSizes1)!=len(skuSizes2)) and ((skuSizes2)!=(skuSizes3) or len(skuSizes2)!=len(skuSizes3)):
                
            
                    datagrouped.iloc[i+1, datagrouped.columns.get_loc("AncienSku")] = ", ".join(row1["ResourceSkuSize"])
                    datagrouped.iloc[i+1, datagrouped.columns.get_loc("NouveauSku")] =  ", ".join(row3["ResourceSkuSize"])
                    datagrouped.iloc[i+2, datagrouped.columns.get_loc("GainRightSizing")] = row3["CostOnDemand"]-row1["CostOnDemand"]

    return datagrouped
    
    if type(gains)!=pd.core.frame.DataFrame:
        gains=data
    else:
        gains=pd.concat([gains,data],ignore_index=True)
    # print(service,data["Gain"].sum())
    # print(gains)
    return gains 

# datagrouped = get_gains_rightsizing(resourcesAzure,"SQL Database")>
donnees = resourcesAzure.copy()     
date_min="2022-10-01"
gains_rightsizing_app_service = get_gains_rightsizing(donnees,"Azure App Service",date_min)
# gains_rightsizing_app_service[["Date","ResourceName","ResourceSkuSize","CostTot","UsageTot","CostOnDemand","UsageOnDemand","UnitCostOnDemand","FacteurRabaisNegociationContrat","UnitCostOnDemandNegocie","GainRightSizing"]]

# %store gains_rightsizing_app_service
# gains_rightsizing_sql_database = get_gains_rightsizing(donnees,"SQL Database",date_min)
# %store gains_rightsizing_sql_database
# gains_rightsizing_app_service
# gains_rightsizing_app_service[(gains_rightsizing_app_service["ResourceName"]=="asp-gb-cmss-prod01")]
# %store gains_rightsizing_app_service


# %%
def plot_cost_rightsizing(donnees,services=None,cumule=False,date_min="2020-01-01"):
    locale.setlocale(locale.LC_ALL, '')
    
    colors=["FF0000","003AFF","23FF00","FFB900","00F3FF","FF0093","00FFB9","8B00FF","FFFF00","FF0097","B6FF00"]
    
    donnees=donnees.rename(columns={"ServiceName":"Service"})
    donnees=donnees[donnees["Date"]>=date_min]
    
    # Convertir la colonne "date" en format de date
    dates=donnees["Date"].unique()
    donnees['Date'] = pd.to_datetime(donnees['Date'])
    
    # Trier le DataFrame par ordre croissant de date
    donnees = donnees.sort_values('Date')

    dates=list(dates)
    dates.sort()
    # Créer une liste de traces pour chaque service
    traces = []

    df_grouped=donnees.groupby(['Date',"Service"])[["CostTot"]].sum().reset_index()
    df_grouped["GainRightSizing"]=df_grouped.apply(lambda row : donnees[(donnees["Date"]==row["Date"])&(donnees["GainRightSizing"]<0)&(donnees["Service"]==row["Service"])]["GainRightSizing"].sum(),axis=1)
  
    if services==None:
        services = df_grouped["Service"].unique()

    max_y = 0
    for index_service in range(len(services)):
        
        service=services[index_service]
        print(service)
        base_color = colors[index_service+3]
        dark_color, light_color = generate_color_shades(base_color)
        

        
        data=df_grouped[df_grouped["Service"]==service]
   
        if not data.empty:
        
            data["Date"]=pd.to_datetime(data["Date"])
            data=data.sort_values("Date")
            
            data["GainRightSizing"]=-data["GainRightSizing"]
            data["GainRightSizingCumule"]=data.apply(lambda row:data[(data["Date"]<=row["Date"]) & (data["GainRightSizing"]>0)]["GainRightSizing"].sum(),axis=1)
            data["CostTotSansGainRightSizingCumule"]=data["CostTot"]+data["GainRightSizingCumule"]
            data["CostTotSansGainRightSizing"]=data["CostTot"]+data["GainRightSizing"]

            # print(data[data["GainRightSizing"]>0]["GainRightSizing"].sum())
            # return data
            # return data
            # return data
            # print(data["GainCumule"].max())
            max_y = max(max_y,data["GainRightSizingCumule"].max())


            show_data=pd.concat([pd.DataFrame.from_dict([{
                    "Date": (data["Date"].iloc[0]- timedelta(days=30)).replace(hour=0).replace(minute=0).replace(second=0).replace(microsecond=0),
                    "Service":service,
                    "CostTot":0,
                    "GainRightSizing":0,
                    "GainRightSizingCumule":0,
                    "CostTotSansGainRightSizing":0,
                    "CostTotSansGainRightSizingCumule":0            
                }]),data.copy()], ignore_index=True)
            show_data=data.copy()
            
            if cumule:
                trace = go.Scatter(
                    x=show_data["Date"],  # Concaténation des coordonnées x dans l'ordre croissant et décroissant
                    y=show_data["CostTotSansGainRightSizingCumule"],  # Ajout de zéros à la fin pour fermer l'aire sous la courbe
                    # fill='tozeroy',  # Remplit l'aire sous la courbe jusqu'à l'axe y=0
                    mode='lines',  # Mode "lines" pour obtenir une ligne continue
                    # fillcolor='rgba(0, 176, 246, 0.2)',  # Couleur de remplissage de l'aire sous la courbe
                    name=service+" projeté",
                    hovertemplate="%{y:,.0f} kCHF",
                    line={"color":light_color}
                )
                traces.append(trace)
            else:
                trace = go.Scatter(
                    x=show_data["Date"],  # Concaténation des coordonnées x dans l'ordre croissant et décroissant
                    y=show_data["CostTotSansGainRightSizing"],  # Ajout de zéros à la fin pour fermer l'aire sous la courbe
                    # fill='tozeroy',  # Remplit l'aire sous la courbe jusqu'à l'axe y=0
                    mode='lines',  # Mode "lines" pour obtenir une ligne continue
                    # fillcolor='rgba(0, 176, 246, 0.2)',  # Couleur de remplissage de l'aire sous la courbe
                    name=service+" projeté",
                    hovertemplate="%{y:,.0f} kCHF",
                    line={"color":"#A37E2C"}
                )
                traces.append(trace)
                
            #Coûts totaux
            trace = go.Scatter(
                x=show_data['Date'],  # Concaténation des coordonnées x dans l'ordre croissant et décroissant
                y=show_data["CostTot"],  # Ajout de zéros à la fin pour fermer l'aire sous la courbe
                # fill='tozeroy',  # Remplit l'aire sous la courbe jusqu'à l'axe y=0
                mode='lines',  # Mode "lines" pour obtenir une ligne continue
                # fillcolor='rgba(0, 176, 246, 0.2)',  # Couleur de remplissage de l'aire sous la courbe
                name=service,
                hovertemplate="%{y:,.0f} kCHF",
                # text=service  # Add the series name as the text for each point
                line={"color":"#006039"}
            )

            traces.append(trace)
            
            bars = go.Bar(
            x=df_grouped['Date'],
            y=abs(df_grouped["GainRightSizing"]),
            # text=round(data['CostTot'], 1),
            hovertemplate='%{y:,.0f}<extra></extra>',  # Modèle de survol avec une seule décimale
             yaxis="y2",
            # width= 5,  # Adjust this value to control spacing between bar groups
            name="Gains",
            marker_color="#A37E2C"
            )
            traces.append(bars)

     # Définir le layout du graphique
    layout = go.Layout(
        barmode='stack',  # Barres empilées
        xaxis=dict(title='Date'),
        yaxis=dict(title="Coût"),
        title='Estimation des gains générés par le changement de sku ',
        legend=dict(x=1.05, y=1, bgcolor='rgba(255, 255, 255, 0.5)'),
        margin=dict(l=75, r=50, t=80, b=80),
        showlegend=True
    )
    
    # Créer la figure
    fig = go.Figure(data=traces, layout=layout)
    fig.update_layout(
        xaxis_range=[min(df_grouped["Date"]),max(df_grouped["Date"])],
        yaxis=dict(title='Coûts réels / Coûts projetés',rangemode='nonnegative'),
        yaxis2=dict(title='Gains rightsizing', overlaying='y', side='right',rangemode='nonnegative',showgrid=False),
        legend=dict(x=1.05, y=1, bgcolor='rgba(255, 255, 255, 0.5)'),
        yaxis_range=[10000, 200000],
        yaxis2_range=[0,60000],
        bargap=0.5
        
    )
    fig.update_xaxes(
        tickmode='array',
        tickvals=df_grouped["Date"],
        tickangle=270,  # Angle de rotation des ticks
        tickformat='%m.%y',  # Format des ticks de date (optionnel)
    )
    
    # Afficher le graphique
    fig.show()
    
    data=data.rename(columns={"Service":"ServiceName"})

    return data
    

# %%
gains_reservation_app_service=get_gains_reservation(donnees,"Azure App Service")
%store gains_reservation_app_service

gains_reservation_sql_database=get_gains_reservation(donnees,"SQL Database")
%store gains_reservation_sql_database

# %%
def plot_cost_reservation(donnees,services=None,cumule=False,date_min="2020-01-01"):
    locale.setlocale(locale.LC_ALL, '') 
    
    colors=["FF0000","003AFF","23FF00","FFB900","00F3FF","FF0093","00FFB9","8B00FF","FFFF00","FF0097","B6FF00"]
    donnees=donnees[donnees["Date"]>=date_min]
    
    
    # Convertir la colonne "date" en format de date
    dates=donnees["Date"].unique()
    donnees['Date'] = pd.to_datetime(donnees['Date'])
    
    # Trier le DataFrame par ordre croissant de date
    donnees = donnees.sort_values('Date')
    

    dates=list(dates)
    dates.sort()
    # Créer une liste de traces pour chaque service
    traces = []

    df_grouped=donnees.groupby(['Date',"ServiceName"])[["CostTot","GainReservation"]].sum().reset_index()
    df_grouped=df_grouped.sort_values("CostTot",ascending=False)
    

    # df_grouped_decom=lignesDecommissionnement.groupby(['Date',"Service"])["CostTot"].sum().reset_index()
    # df_grouped_decom=df_grouped_decom.sort_values("CostTot",ascending=False)
    
    # return df_grouped_decom.sort_values()
    # if cumule:
    # #     showMetric="Sum"+showMetric
    if services==None:
        services = df_grouped["ServiceName"].unique()

    
    
    max_y = 0
    for index_service in range(len(services)):
        service=services[index_service]
        base_color = colors[index_service]
        dark_color, light_color = generate_color_shades(base_color)
        
        data=df_grouped[df_grouped["ServiceName"]==service]
        # return data

        # return data§
        max_y = max(max_y,data["GainReservation"].max())
          
        data["GainReservationCumule"]=data["Date"].apply(lambda date:data[data["Date"]<=date]["GainReservation"].sum())
         
        data["CostTotSansReservation"]=data["CostTot"]+data["GainReservation"]
        
        data["CostTotSansReservationCumule"]=data["CostTot"]+data["GainReservationCumule"]

        data=data.sort_values("Date")
        

        if cumule:
            trace = go.Scatter(
                x=data["Date"],  # Concaténation des coordonnées x dans l'ordre croissant et décroissant
                y=data["CostTotSansReservationCumule"],  # Ajout de zéros à la fin pour fermer l'aire sous la courbe
                # fill='tozeroy',  # Remplit l'aire sous la courbe jusqu'à l'axe y=0
                mode='lines',  # Mode "lines" pour obtenir une ligne continue
                # fillcolor='rgba(0, 176, 246, 0.2)',  # Couleur de remplissage de l'aire sous la courbe
                name=service+" projeté sans réservation cumulé",
                hovertemplate="%{y:,.0f} kCHF",
                line={"color":light_color}
            )
        else:
            trace = go.Scatter(
                x=data["Date"],  # Concaténation des coordonnées x dans l'ordre croissant et décroissant
                y=data["CostTotSansReservation"],  # Ajout de zéros à la fin pour fermer l'aire sous la courbe
                # fill='tozeroy',  # Remplit l'aire sous la courbe jusqu'à l'axe y=0
                mode='lines',  # Mode "lines" pour obtenir une ligne continue
                # fillcolor='rgba(0, 176, 246, 0.2)',  # Couleur de remplissage de l'aire sous la courbe
                name=service+" projeté sans réservation",
                hovertemplate="%{y:,.0f} kCHF",
                line={"color":"#002060"}
            )

        
        
        traces.append(trace)
        #Coûts totaux
        trace = go.Scatter(
            x=data['Date'],  # Concaténation des coordonnées x dans l'ordre croissant et décroissant
            y=data["CostTot"],  # Ajout de zéros à la fin pour fermer l'aire sous la courbe
            # fill='tozeroy',  # Remplit l'aire sous la courbe jusqu'à l'axe y=0
            mode='lines',  # Mode "lines" pour obtenir une ligne continue
            # fillcolor='rgba(0, 176, 246, 0.2)',  # Couleur de remplissage de l'aire sous la courbe
            name=service,
            hovertemplate="%{y:,.0f} kCHF",
            # text=service  # Add the series name as the text for each point
            line={"color":"#006039"}
        )

        
        traces.append(trace)

        
    bars = go.Bar(
            x=df_grouped['Date'],
            y=abs(df_grouped["GainReservation"]),
            # text=round(data['CostTot'], 1),
            hovertemplate='%{y:,.0f}<extra></extra>',  # Modèle de survol avec une seule décimale
             yaxis="y2",
            # width= 5,  # Adjust this value to control spacing between bar groups
            name="Gains",
            marker_color="#002060"
            )
    traces.append(bars)
        
     # Définir le layout du graphique
    layout = go.Layout(
        barmode='stack',  # Barres empilées
        xaxis=dict(title='Date'),
        yaxis=dict(title="Coût"),
        title='Coûts projetés en cas de non réservation Azure App Service',
        legend=dict(x=1.05, y=1, bgcolor='rgba(255, 255, 255, 0.5)'),
        margin=dict(l=75, r=50, t=80, b=80),
        showlegend=True
    )
    
    # Créer la figure
    fig = go.Figure(data=traces, layout=layout)
    fig.update_layout(
        xaxis_range=[min(data["Date"]),max(data["Date"])],
        yaxis=dict(title='Coûts réels / Coûts projetés',rangemode='nonnegative'),
        yaxis2=dict(title='Gains réservation', overlaying='y', side='right',rangemode='nonnegative',showgrid=False),
        legend=dict(x=1.05, y=1, bgcolor='rgba(255, 255, 255, 0.5)'),
        yaxis_range=[80000, 200000],
        yaxis2_range=[0,40000],
        bargap=0.5
        
    )
    
    fig.update_xaxes(
        tickmode='array',
        tickvals=df_grouped["Date"],
        tickangle=270,  # Angle de rotation des ticks
        tickformat='%m.%y',  # Format des ticks de date (optionnel)
    )
    
    
    # Afficher le graphique
    fig.show()
    return data

plot_cost_reservation(gains_reservation_app_service,services=["Azure App Service"])

    

# %%
gains_reservation_app_service[(gains_reservation_app_service["GainReservation"]>0)&(gains_reservation_app_service["ResourceName"]=="asp-zz-cmss-prod01")][["Date","ResourceName","ResourceSkuSize","UsageTot","CostTot","UsageOnDemand","CostOnDemand","UsageReservation1Y","CostReservation1Y","UnitCostOnDemand","UnitCostReservation1Y","GainReservation"]]

# %%
def plot_cost_gains_tot(donnees,gains_decommissionnement,gains_rightsizing,gains_reservation,services=None,cumule=False,date_min="2022-01-01",journalier=False):
    locale.setlocale(locale.LC_ALL, '')
    
    colors=["FF0000","003AFF","23FF00","FFB900","00F3FF","FF0093","00FFB9","8B00FF","FFFF00","FF0097","B6FF00"]
    
    grapheCostDecom = plot_cost_decom(donnees.copy(),gains_decommissionnement.copy(),services=services,date_min=date_min)
    grapheCostRightSizing=plot_cost_rightsizing(gains_rightsizing.copy(),services=services,cumule=False,date_min=date_min)
    grapheCostReservation = plot_cost_reservation(gains_reservation.copy(),services=services,cumule=False,date_min=date_min)
    

    
    df_merged = pd.merge(grapheCostDecom,grapheCostRightSizing, on=['Date',"ServiceName"])
    
    df_merged = pd.merge(df_merged,grapheCostReservation, on=['Date',"ServiceName"])
    
    df_merged["Check"]=df_merged.apply(lambda row:float(row["CostTot_x"])==float(row["CostTot_y"] ) and float(row["CostTot_y"])==float(row["CostTot"]),axis=1)
    # return df_merged.sort_values("Date")

    df_merged=df_merged.fillna(0)
    
    df_merged["CostTotSansOptimisation"]=df_merged["CostTotSansDecomCumule"]+df_merged["GainReservation"]+df_merged["GainRightSizing"]
    df_merged["CostTot"]=df_merged["CostTot_x"]
    df_merged["GainsTotaux"]=df_merged["CostTotSansOptimisation"]-df_merged["CostTot_x"]
    
    
    if services==None:
        services = df_merged["ServiceName"].unique()

    traces=[]
    
    
    max_y = data["CostTot"].max()
    for index_service in range(len(services)):
        service=services[index_service]
        base_color = colors[index_service]
        dark_color, light_color = generate_color_shades(base_color)
        
        data=df_merged[df_merged["ServiceName"]==service]
        data=data[data["Date"]>=date_min]
        
        # return data§
        max_y = max(max_y,data["CostTotSansOptimisation"].max())
          
        data=data.sort_values("Date")
        
        trace = go.Scatter(
            x=data["Date"],  # Concaténation des coordonnées x dans l'ordre croissant et décroissant
            y=data["CostTotSansOptimisation"],  # Ajout de zéros à la fin pour fermer l'aire sous la courbe
            # fill='tozeroy',  # Remplit l'aire sous la courbe jusqu'à l'axe y=0
            mode='lines',  # Mode "lines" pour obtenir une ligne continue
            # fillcolor='rgba(0, 176, 246, 0.2)',  # Couleur de remplissage de l'aire sous la courbe
            name=service+" projeté sans optimisation",
            hovertemplate="%{y:,.0f} kCHF",
            line={"color":"#804848"}
        )

    
        
        traces.append(trace)
        #Coûts totaux
        trace = go.Scatter(
            x=data['Date'],  # Concaténation des coordonnées x dans l'ordre croissant et décroissant
            y=data["CostTot_x"],  # Ajout de zéros à la fin pour fermer l'aire sous la courbe
            # fill='tozeroy',  # Remplit l'aire sous la courbe jusqu'à l'axe y=0
            mode='lines',  # Mode "lines" pour obtenir une ligne continue
            # fillcolor='rgba(0, 176, 246, 0.2)',  # Couleur de remplissage de l'aire sous la courbe
            name=service,
            hovertemplate="%{y:,.0f} kCHF",
            # text=service  # Add the series name as the text for each point
            line={"color":"#006039"}
        )

        
        traces.append(trace)

       
    
     # Définir le layout du graphique
    layout = go.Layout(
        barmode='stack',  # Barres empilées
        xaxis=dict(title='Date'),
        yaxis=dict(title="Coût"),
        title='Coûts projetés sans optimisations Azure App Service',
        legend=dict(x=1.05, y=1, bgcolor='rgba(255, 255, 255, 0.5)'),
        margin=dict(l=75, r=50, t=80, b=80),
        showlegend=True
    )
    
    # Créer la figure
    fig = go.Figure(data=traces, layout=layout)
    fig.update_layout(xaxis_range=[min(data["Date"]),max(data["Date"])])
    
    fig.update_xaxes(
        tickmode='array',
        tickvals=data["Date"],
        tickangle=270,  # Angle de rotation des ticks
        tickformat='%m.%y',  # Format des ticks de date (optionnel)
    )
    
    
    # Afficher le graphique
    fig.show()
    
    
    
    
    traces=[]
    
    #Gains cumulés
    layout = go.Layout(
        barmode='stack',  # Barres empilées
        xaxis=dict(title='Date'),
        yaxis=dict(title="Coût"),
        title='Gains cumulés Azure App Service',
        legend=dict(x=1.05, y=1, bgcolor='rgba(255, 255, 255, 0.5)'),
        margin=dict(l=75, r=50, t=80, b=80),
        showlegend=True
    )
    
    #Gains décommissionnement
    bars = go.Bar(
            x=data['Date'],
            y=abs(data["GainsTotaux"])-(data["GainReservation"]+data["GainRightSizing"]),
            # text=round(data['CostTot'], 1),
            hovertemplate='%{y:,.0f}<extra></extra>',  # Modèle de survol avec une seule décimale
            #  yaxis="y2",
            # width= 5,  # Adjust this value to control spacing between bar groups
            name="Gains décommissionnement",
            marker_color="#8C99A0"
            )
    traces.append(bars)
    
    
    #Gains rightsizing    
    bars = go.Bar(
            x=data['Date'],
            y=abs(data["GainRightSizingCumule"]),
            # text=round(data['CostTot'], 1),
            hovertemplate='%{y:,.0f}<extra></extra>',  # Modèle de survol avec une seule décimale
            #  yaxis="y2",
            # width= 5,  # Adjust this value to control spacing between bar groups
            name="Gains rightsizing",
            marker_color="#A37E2C"
            )
    traces.append(bars)
    
    #Gains réservation    
    bars = go.Bar(
            x=data['Date'],
            y=abs(data["GainReservationCumule"]),
            # text=round(data['CostTot'], 1),
            hovertemplate='%{y:,.0f}<extra></extra>',  # Modèle de survol avec une seule décimale
            #  yaxis="y2",
            # width= 5,  # Adjust this value to control spacing between bar groups
            name="Gains réservation",
            marker_color="#002060"
            )
    traces.append(bars)
    
    
     
    
    fig = go.Figure(data=traces, layout=layout)
    fig.update_layout(xaxis_range=[min(data["Date"]),max(data["Date"])],
                       yaxis_range=[80000, 200000],
           bargap=0.5
                      )
    
    fig.update_xaxes(
        tickmode='array',
        tickvals=data["Date"],
        tickangle=270,  # Angle de rotation des ticks
        tickformat='%m.%y',  # Format des ticks de date (optionnel)
    )
    
    
    # Afficher le graphique
    fig.show()
    
    return data
    
    
    
    
    
    
# services=["Azure App Service"]
# date_min="2022-10-01"

plot_gains_tot(gains_tot_sql_database_CoreModel.copy(),services=["SQL Database"])
# plot_cost_gains_tot(gains_decommissionnement,gains_rightsizing_app_service,gains_reservation_app_service,services=services,date_min=date_min)


# %% [markdown]
# Unattached Disques

# %%
def get_unattached_azure_disks():
    dates=sorted(list(resourcesAzure.copy()["Date"].unique()))
    final_date=dates[-1]
    
    list_subscriptions=resourcesAzure[resourcesAzure["Date"]==final_date]["ProjectId"].unique()
    
    
    disks=pd.DataFrame(columns=["Date","ProjectId","ProjectName","ResourceGroupName","LocationName","ResourceName","ResourceSkuSize","CostTot","PricingModels","CostTot","PricingModels"])
    counter=0
    
    # list_subscriptions=["00fd21d5-9fda-4aaa-88f5-1b88f385006c"]
    for subscriptionId in list_subscriptions:
        url=f"https://management.azure.com/subscriptions/{subscriptionId}/providers/Microsoft.Compute/disks?api-version=2023-04-02"
        
        while len(url)>0:
            print(counter,url)
            response=doRequest("GET",url,"")

            if not response == None and response.status_code==200:
                data=response.json()
                                
                if len(data["value"])>0:
                    rowDisks=data["value"]
                    
                    df_disks=pd.DataFrame(rowDisks)
                    df_disks["Date"]=final_date
                    df_disks["ProjectId"]=df_disks["id"].apply(lambda val:val.split("/")[2])
                    df_disks["ResourceGroupName"]=df_disks["id"].apply(lambda val:val.split("/")[4])
                    df_disks["ResourceName"]=df_disks["name"]
                    df_disks["LocationName"]=df_disks["location"]
                    df_disks["ResourceSku"]=df_disks["sku"].apply(lambda row:row["tier"])
                    df_disks["ResourceSkuSize"]=df_disks["sku"].apply(lambda row:row["name"])
                    df_disks["TimeCreated"]=df_disks["properties"].apply(lambda row:row["timeCreated"])
                    df_disks["DiskState"]=df_disks["properties"].apply(lambda row:row["diskState"])

                    disks=pd.concat([disks,df_disks[df_disks["DiskState"]=="Unattached"].merge(resourcesAzure,on=["Date","ProjectId","ResourceName","LocationName"],how="inner",suffixes=["x",""])[["Date","ProjectId","ProjectName","ResourceGroupName","LocationName","ResourceName","ResourceSkuSize","CostTot","PricingModels","CostTot","PricingModels"]]])

                    if "nextLink" in data.keys():
                        url=data["nextLink"]
                        
                    else:
                        url=""
                    counter+=1
                    
                else:
                    url=""
            else:
                url=""

    return disks

disks=get_unattached_azure_disks()
# disks.to_excel("C:\\Users\\hugo.dufrene\\Wavestone\\WE - ROLEX [CH] - Support mise en place Gouvernance Cloud public - 13665 - Documents partages\\Gouvernance Cloud\\FinOps\\Optimisations\\Décomissionnement\\Unattached Disks Septembre.xlsx",encoding="utf-8",index=False)
disks


# %% [markdown]
# AZURE APP SERVICE INUTILISEES

# %%


# %%


# %% [markdown]
# AZURE DATA FACTORY

# %%


# %%


# %% [markdown]
# VIRTUAL MACHINES

# %%
def get_Virtual_Machines_Maximums(resourcePath,index,startTime=datetime(2023,1,1,00,00,00), endTime=datetime(2023,9,1,0,0,0)):
    
    print (index,resourcePath)
    
    # CPU Percentage
    api_url = f"https://management.azure.com{resourcePath}/providers/microsoft.insights/metrics?api-version=2019-07-01&metricnames=Percentage CPU&aggregation=maximum&interval=P1D&timespan="+startTime.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+endTime.strftime("%Y-%m-%dT%H:%M:%SZ")
    response = doRequest("GET",api_url,"")

    

    if not response==None and not response.status_code==404:
        if not response==None and response.status_code == 200 :
            data = response.json()
            # return data
            if "value" in data.keys() and len(data["value"])>0 and "timeseries" in data["value"][0] and len(data["value"][0]["timeseries"])>0  and "data" in data["value"][0]["timeseries"][0]:
                compteur=pd.DataFrame.from_dict(data["value"][0]["timeseries"][0]["data"]) #CPU TIME
                # print(compteur.head())
                
                compteur["Date"]=compteur["timeStamp"].apply(lambda row:row.split("T")[0])
                compteur["Heure"]=compteur["timeStamp"].apply(lambda row:row.split("T")[1])
                compteur=compteur.fillna(0.0)
                compteur=compteur.rename(columns={"maximum":"PercentageCPU"})
                
                
                if not "PercentageCPU" in compteur.head():
                    compteur["PercentageCPU"]=compteur.apply(lambda r : 0.0)
                
                PercentageCPU=max(compteur["PercentageCPU"])

            else:
                PercentageCPU=0.0
            
        else:
            PercentageCPU=None
            print("Erreur lors de l'appel à l'API:", response)
        # return MaxCPUTime

        # if not MaxCPUTime==None:
        #OS DISK READ
        api_url = f"https://management.azure.com{resourcePath}/providers/microsoft.insights/metrics?api-version=2019-07-01&metricnames=Disk Read Bytes&aggregation=maximum&interval=P1D&timespan="+startTime.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+endTime.strftime("%Y-%m-%dT%H:%M:%SZ")
        response = doRequest("GET",api_url,"")

        if not response==None and response.status_code == 200 :
            data = response.json()
            
            if "value" in data.keys() and len(data["value"])>0 and "timeseries" in data["value"][0] and len(data["value"][0]["timeseries"])>0  and "data" in data["value"][0]["timeseries"][0]:
                compteur=pd.DataFrame.from_dict(data["value"][0]["timeseries"][0]["data"]) #Max Requests
                # print(compteur.head())
                
                compteur["Date"]=compteur["timeStamp"].apply(lambda row:row.split("T")[0])
                compteur["Heure"]=compteur["timeStamp"].apply(lambda row:row.split("T")[1])
                compteur=compteur.fillna(0.0)
                compteur=compteur.rename(columns={"maximum":"OSDiskReadBytesPerSec"})
                
                
                if not "OSDiskReadBytesPerSec" in compteur.head():
                    compteur["OSDiskReadBytesPerSec"]=compteur.apply(lambda r : 0.0)
                
                OSDiskReadBytesPerSec=max(compteur["OSDiskReadBytesPerSec"])
            else:
                OSDiskReadBytesPerSec=0.0
        else:
            OSDiskReadBytesPerSec=0.0
            print("Erreur lors de l'appel à l'API:", response)
        

        #MaxMemoryWorkingSet
        api_url = f"https://management.azure.com{resourcePath}/providers/microsoft.insights/metrics?api-version=2019-07-01&metricnames=Disk Write Bytes&aggregation=maximum&interval=P1D&timespan="+startTime.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+endTime.strftime("%Y-%m-%dT%H:%M:%SZ")
        response = doRequest("GET",api_url,"")

        if not response==None and response.status_code == 200 :
            data = response.json()
            
            if "value" in data.keys() and len(data["value"])>0 and "timeseries" in data["value"][0] and len(data["value"][0]["timeseries"])>0  and "data" in data["value"][0]["timeseries"][0]:
                
                compteur=pd.DataFrame.from_dict(data["value"][0]["timeseries"][0]["data"]) #Session count
                # print(compteur.head())
                
                compteur["Date"]=compteur["timeStamp"].apply(lambda row:row.split("T")[0])
                compteur["Heure"]=compteur["timeStamp"].apply(lambda row:row.split("T")[1])
                compteur=compteur.fillna(0.0)
                compteur=compteur.rename(columns={"maximum":"OSDiskWriteBytesPerSec"})
                
                
                if not "OSDiskWriteBytesPerSec" in compteur.head():
                    compteur["OSDiskWriteBytesPerSec"]=compteur.apply(lambda r : 0.0)
                
                OSDiskWriteBytesPerSec=max(compteur["OSDiskWriteBytesPerSec"])  
                
            else:
                OSDiskWriteBytesPerSec=0.0  
            
            # return compteur[["Date","Heure","MaxSuccessfulConnection"]]
        else:
            OSDiskWriteBytesPerSec=0.0
            print("Erreur lors de l'appel à l'API:", response)

        #FunctionExecutionCount
        # api_url = f"https://management.azure.com{resourcePath}/providers/microsoft.insights/metrics?api-version=2019-07-01&metricnames=IntegrationRuntimeCpuPercentage&aggregation=maximum&interval=P1D&timespan="+startTime.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+endTime.strftime("%Y-%m-%dT%H:%M:%SZ")
        # response = doRequest("GET",api_url,"")

        # if not response==None and response.status_code == 200 :
        #     data = response.json()
            
        #     if "value" in data.keys() and len(data["value"])>0 and "timeseries" in data["value"][0] and len(data["value"][0]["timeseries"])>0  and "data" in data["value"][0]["timeseries"][0]:
                
        #         compteur=pd.DataFrame.from_dict(data["value"][0]["timeseries"][0]["data"]) #Session count
        #         # print(compteur.head())
                
        #         compteur["Date"]=compteur["timeStamp"].apply(lambda row:row.split("T")[0])
        #         compteur["Heure"]=compteur["timeStamp"].apply(lambda row:row.split("T")[1])
        #         compteur=compteur.fillna(0.0)
        #         compteur=compteur.rename(columns={"maximum":"IntegrationRuntimeCpuPercentage"})
                
                
        #         if not "IntegrationRuntimeCpuPercentage" in compteur.head():
        #             compteur["IntegrationRuntimeCpuPercentage"]=compteur.apply(lambda r : 0.0)
                
        #         IntegrationRuntimeCpuPercentage=max(compteur["IntegrationRuntimeCpuPercentage"])  
                
        #     else:
        #         IntegrationRuntimeCpuPercentage=0.0  
            
        #     # return compteur[["Date","Heure","MaxSuccessfulConnection"]]
        # else:
        #     IntegrationRuntimeCpuPercentage=0.0
           
        #     print("Erreur lors de l'appel à l'API:", response)
        # # return MaxMemoryWorkingSet
        # if math.isnan(IntegrationRuntimeCpuPercentage):
        #     IntegrationRuntimeCpuPercentage=0.0
                
        return PercentageCPU,OSDiskReadBytesPerSec,OSDiskWriteBytesPerSec
    return None

start_date=datetime(2023,9,1,0,0,0,0)
end_date=datetime(2023,9,30,23,59,59,999)
get_Virtual_Machines_Maximums('/subscriptions/71d1e827-fa8e-405a-8e9c-435c24c73359/resourcegroups/rg-xc-infra-backup/providers/microsoft.compute/virtualmachines/saz-xc-0079',0,start_date,end_date)
  

# %%
def get_Virtual_Machines_Metrics():
    dates=sorted(list(resourcesAzure.copy()["Date"].unique()))
    final_date=dates[-1]
    
    list_subscriptions=resourcesAzure[resourcesAzure["Date"]==final_date]["ProjectId"].unique()
    
    date1="2023-08-01"
    start_date1=datetime(2023,8,1,0,0,0,0)
    end_date1=datetime(2023,8,31,23,59,59,999)
    

    date2="2023-09-01"
    start_date2=datetime(2023,9,1,0,0,0,0)
    end_date2=datetime(2023,9,30,23,59,59,999)
    # subscriptionId=list_subscriptions[0]
    
    
    vms=pd.DataFrame(columns=["Date","ProjectId","ProjectName","ResourceGroupName","LocationName","ResourceName","ResourceSkuSize","CostTot","PricingModels"])
    counter=0
    
    # list_subscriptions=["669420be-9749-47c9-be7e-8b7c6f042a25"]
    for subscriptionId in list_subscriptions:
        url=f"https://management.azure.com/subscriptions/{subscriptionId}/providers/Microsoft.Compute/virtualMachines?api-version=2023-07-01"
        
        while not url == None and len(url)>0:
            print(counter,url)
            response=doRequest("GET",url,"")

            if not response == None and response.status_code==200:
                data=response.json()
                                
                if len(data["value"])>0:
                    rowVMS=data["value"]
                    
                    # return rowVMS[0]["properties"]["timeCreated"]
                    
                    df_vms=pd.DataFrame(rowVMS)
                    df_vms["Date"]=final_date
                    df_vms["ProjectId"]=df_vms["id"].apply(lambda val:val.split("/")[2])
                    df_vms["ResourceGroupName"]=df_vms["id"].apply(lambda val:val.split("/")[4])
                    df_vms["ResourceName"]=df_vms["name"]
                    df_vms["LocationName"]=df_vms["location"]
                    df_vms["VmSize"]=df_vms["properties"].apply(lambda row:row["hardwareProfile"]["vmSize"])
                    df_vms["CreatedTime"]=df_vms["properties"].apply(lambda row:row["timeCreated"])

                                        
                    # return df_vms
                    
                    virtual_machines= df_vms.merge(resourcesAzure.copy(),on=["Date","ProjectId","ResourceName"],how="inner",suffixes=["x",""])[["Date","ProjectId","ProjectName","ResourceGroupName","LocationName","ServiceName","ServiceTier","ResourceName","ResourcePath","ResourceSkuSize","CostTot","PricingModels"]]
                    # return virtual_machines
                    virtual_machines=virtual_machines[virtual_machines["ServiceName"]=="Virtual Machines"]
                    
                    # return sites
                    vms=pd.concat([vms,virtual_machines])

                    # return azure_apps
                   # df_apps["ResourceSku"]=df_apps["sku"].apply(lambda row:row["properties"][1]["Reserved"])
                    # df_disks["ResourceSkuSize"]=df_disks["sku"].apply(lambda row:row["name"])
                    # df_disks["TimeCreated"]=df_disks["properties"].apply(lambda row:row["timeCreated"])
                    # df_disks["DiskState"]=df_disks["properties"].apply(lambda row:row["diskState"])

                    # disks=pd.concat([disks,df_disks[df_disks["DiskState"]=="Unattached"].merge(resourcesAzure,on=["Date","ProjectId","ResourceName","LocationName"],how="inner",suffixes=["x",""])[["Date","ProjectId","ProjectName","ResourceGroupName","LocationName","ResourceName","ResourceSkuSize","CostTot","PricingModels","CostTot","PricingModels"]]])


                    if "nextLink" in data.keys():
                        url=data["nextLink"]

                    else:
                        url=""
                    
                    
                else:
                    url=""
            else:
                url=""
                
            counter+=1
            
    vms = vms.groupby('ResourceName').agg({
        'Date': 'first',  # Prenez la première date
        'ProjectId': 'first',  # Prenez la première ProjectId
        'ProjectName': 'first',  # Prenez la première ProjectName
        'ResourceGroupName': 'first',  # Prenez la première ResourceGroupName
        'LocationName': 'first',  # Prenez la première LocationName
        'ServiceName': 'first',  # Prenez la première ServiceName
        'ServiceTier': agg_func,  # Prenez la première ServiceTier
        'ResourceSkuSize': agg_func,  # Prenez la première ResourceSkuSize
        'ResourcePath': 'first',  # Prenez la première ResourcePath
        'PricingModels': agg_func,  # Prenez la première PricingModels
        'CostTot': 'sum'  # Sommez les coûts
    }).reset_index()
    
    vms["CheckNamingConvention"]=vms["ResourceName"].apply(lambda row:check_naming_convention(row))
    # return vms
    vms["Perfs "+date1]=vms.apply(lambda row:get_Virtual_Machines_Maximums(row["ResourcePath"],row.name,start_date1,end_date1),axis=1)
    vms["Perfs "+date2]=vms.apply(lambda row:get_Virtual_Machines_Maximums(row["ResourcePath"],row.name,start_date2,end_date2),axis=1)
    
    vms["PercentageCPU "+date1] = vms["Perfs "+date1].apply(lambda row: row[0] if (isinstance(row, tuple) and len(row) > 0) else 0)
    vms["OSDiskReadBytesPerSec "+date1]=vms["Perfs "+date1].apply(lambda row:row[1] if (isinstance(row, tuple) and len(row) > 0) else 0)
    vms["OSDiskWriteBytesPerSec "+date1]=vms["Perfs "+date1].apply(lambda row:row[2] if (isinstance(row, tuple) and len(row) > 0) else 0)
    # dataFactory["IntegrationRuntimeCpuPercentage "+date1]=vms["Perfs "+date1].apply(lambda row:row[3] if (isinstance(row, tuple) and len(row) > 0) else 0)

    vms["PercentageCPU "+date2]=vms["Perfs "+date2].apply(lambda row:row[0] if (isinstance(row, tuple) and len(row) > 0) else 0)
    vms["OSDiskReadBytesPerSec "+date2]=vms["Perfs "+date2].apply(lambda row:row[1] if (isinstance(row, tuple) and len(row) > 0) else 0)
    vms["OSDiskWriteBytesPerSec "+date2]=vms["Perfs "+date2].apply(lambda row:row[2] if (isinstance(row, tuple) and len(row) > 0) else 0)
    # dataFactory["IntegrationRuntimeCpuPercentage "+date2]=dataFactory["Perfs "+date2].apply(lambda row:row[3] if (isinstance(row, tuple) and len(row) > 0) else 0)
                    # PercentageCPU,OSDiskReadBytesPerSec,OSDiskWriteBytesPerSec


    vms.to_excel(f"C:\\Users\\hugo.dufrene\\Wavestone\\WE - ROLEX [CH] - Support mise en place Gouvernance Cloud public - 13665 - Documents partages\\Gouvernance Cloud\\FinOps\\Optimisations\\Décomissionnement\\Virtual Machines {date1} {date2}.xlsx",encoding="utf-8",index=False)
    return vms

vms=get_Virtual_Machines_Metrics()
# disks.to_excel("C:\\Users\\hugo.dufrene\\Wavestone\\WE - ROLEX [CH] - Support mise en place Gouvernance Cloud public - 13665 - Documents partages\\Gouvernance Cloud\\FinOps\\Optimisations\\Décomissionnement\\Unattached Disks Septembre.xlsx",encoding="utf-8",index=False)
vms

# %%
vms[vms["ResourceName"]=="saz-xd-0114-1"]

# %%
vms[vms["ResourceName"]=="saz-xd-0114-1"]

# %% [markdown]
# LEGACY

# %%
def get_ressources_legacy(subscriptions,lignesFacturation):

    lignesFacturation.sort(key=lambda x:(x["ResourceId"],x["Date"]))
    
    resources =[]
    for indiceLigne in range(len(lignesFacturation)):
        ligne=lignesFacturation[indiceLigne]
        newResource=True
        indice=None
        resource=None
        for i in range(len(resources)) : 
            res = resources[len(resources)-i-1]
            if res["Date"]==ligne["Date"] and res["Project"]["SubscriptionId"]==ligne["Subscription"] and res["Project"]["ResourceGroupName"]==ligne["ResourceGroup"] and res["Resource"]["ServiceName"]==ligne["Service"] and res["Resource"]["ResourceId"]==ligne["ResourceId"]:
                newResource=False
                indice=len(resources)-i-1
                resource=res
                break
            
            if i>20:
                break
            
            
              
        if newResource and resource==None:
            application=get_application(subscriptions,ligne["Subscription"],ligne["ResourceGroup"])
            location=get_infos_location(subscriptions,ligne["Subscription"],ligne["Location"])
            service=get_infos_resource(ligne["Service"],ligne["ResourceId"],ligne["ResourceType"])
        
            resource = {
                "Date":ligne["Date"],
                "Project":{
                    "Provider":"Azure",
                    
                    "SubscriptionId":ligne["Subscription"], #Azure
                    "SubscriptionName":application["Abonnement"],
                    "ResourceGroupName":ligne["ResourceGroup"],
                    
                    "ProjectId":None, #GCP
                    "ProjectName":None,
                    
                    "ApplicationName":application["Application"],
                    "ProductOwner":application["ProductOwner"]
                },
                "Location":{
                    "LocationId":None,                
                    "LocationName":None,
                    "LocationsubscriptionName":None,
                    "LocationRegionalDisplayName":None
                },

                "Resource":{
                    "ResourceId":service["ResourceId"],
                    "ResourceName":service["ResourceName"].lower(),#TODO
                    "ResourceType":service["ResourceType"],
                    
                    "ServiceId":None, #TODO
                    "ServiceName":service["ServiceName"],
                    "ServiceType":service["ServiceType"], 
                    "ServiceInfraType":service["ServiceInfraType"], 

                    "Environment":None, #TODO
                    
                    "ResourceSku":{
                        "OS":None, #TODO
                        "Ids":None,
                        "Tiers":[],
                        "Sizes":[] #TODO
                    },
                    "ResourceTags":[], #TODO
                    "ResourceConfiguration":{} #TODO
                },
                
                "PricingModel":{
                    "Models":[],
                    "ChargeTypes":[],
                    "Currency":ligne["Currency"],
                    "Unit":None, #TODO
                    "DiscountPercentage":None, #TODO
                    "DiscountSource":None #TODO
                },
                
                "UnitPrices":{
                    "OnDemand":None, #TODO
                    "Reservation1Y":None, #TODO
                    "Reservation3Y":None, #TODO
                    "DevTest":None #TODO
                },
                
                "Usages":{
                    "Usage":0, 
                    "UsageJ":0, #TODO à revoir journalier
                    "UsageOnDemand":0, #TODO
                    "UsageOnDemandJ":0, #TODO
                    "UsageReservation1Y":0, #TODO
                    "UsageReservation1YJ":0, #TODO
                    "UsageReservation3Y":0, #TODO
                    "UsageReservation3YJ":0, #TODO
                    "UsageDevTest":0, #TODO
                    "UsageDevTestJ":0, #TODO
                    "UsageSpot":0, #TODO
                    "UsageSpotJ":0
                },
                
                "Costs":{
                    "Cost":0, 
                    "CostJ":0, #TODO à revoir journalier
                    "CostOnDemand":0, #TODO
                    "CostOnDemandJ":0, #TODO
                    "CostReservation1Y":0, #TODO
                    "CostReservation1YJ":0, #TODO
                    "CostReservation3Y":0, #TODO
                    "CostReservation3YJ":0, #TODO
                    "CostDevTest":0, #TODO
                    "CostDevTestJ":0, #TODO 
                    "CostSpot":0, #TODO
                    "CostSpotJ":0 #TODO 
                },
                        
                "Gains":{
                    "AutoShutdown":None,#TODO
                    "Reservation":None,#TODO
                    "Commitment":None,#TODO
                    "Spot":None#TODO
                },
                
                "ComputeData":{},
                "StorageData":{},
                "NetworkData":{},
                "SecurityData":{},
                
                "Recomendations":[]
            }
            
            
            
        if location!=None:
            resource["Location"]={
                    "LocationId":location["id"],                
                    "LocationName":location["name"],
                    "LocationDisplayName":location["displayName"],
                    "LocationRegionalDisplayName":location["regionalDisplayName"]
                }
        
        resource["Usages"]["Usage"]+=ligne["Usage"]
        resource["Usages"]["UsageJ"]=resource["Usages"]["Usage"]/ligne["JoursMois"]
        
        resource["Costs"]["Cost"]+=ligne["Coût"] #TODO à condition que ce soit la même devise
        resource["Costs"]["CostJ"]=resource["Costs"]["Cost"]/ligne["JoursMois"] #TODO à condition que ce soit la même devise

        if ligne["PricingModel"]=="Reservation":
            if not ligne["Reservation"]==None and ("_1y".lower() in ligne["Reservation"].lower() or "_y".lower() in ligne["Reservation"].lower()) :
                model="Reservation1Y"
            elif not ligne["Reservation"]==None and "_3y".lower() in ligne["Reservation"].lower():
                model="Reservation3Y"
            else:
                model=None
                print("Erreur",ligne["Reservation"])
        elif ligne["PricingModel"]=="Spot":
            model="Spot"
            
        else:
            model="OnDemand"
        
        if not model in resource["PricingModel"]["Models"]:
            resource["PricingModel"]["Models"].append(model)
        
        resource["Usages"]["Usage"+model]+=ligne["Usage"]
        resource["Usages"]["Usage"+model+"J"]=resource["Usages"]["Usage"+model]/ligne["JoursMois"]
        
        resource["Costs"]["Cost"+model]+=ligne["Coût"] #TODO à condition que ce soit la même devise
        resource["Costs"]["Cost"+model+"J"]=resource["Costs"]["Cost"+model]/ligne["JoursMois"] #TODO à condition que ce soit la même devise       
            
        if not ligne["ChargeType"] in resource["PricingModel"]["ChargeTypes"]:
            resource["PricingModel"]["ChargeTypes"].append(ligne["ChargeType"])
        
        if not ligne["ServiceTier"] in resource["Resource"]["ResourceSku"]["Tiers"]:
            resource["Resource"]["ResourceSku"]["Tiers"].append(ligne["ServiceTier"])
        
        
        if newResource:
            resources.append (resource)
            
        else:
            resources[indice]=resource
        
        if indiceLigne%1000==0:
            print("Avancement","{:.2%}".format(indiceLigne/len(lignesFacturation))," %", len(resources))
            
            
    return resources

#token=getToken()
#resourcesAzure= get_ressources_legacy(subscriptions,lignesFacturationAzure)
#%store resourcesAzure

# %%
def get_subscriptions_legacy():
     # Récupération de la liste des abonnements Azure
    url = "https://management.azure.com/subscriptions?api-version=2022-12-01"
    response = doRequest("GET",url, "")
    subscriptions= response.json()["value"]
    for i in range(len(subscriptions)):
        subscriptions[i]["locations"]=get_locations_subscription(subscriptions[i]["subscriptionId"])
    return  subscriptions

def get_infos_subscription_legacy(subscriptions,recherche):
    listeRecherche=[]
    
    for sub in subscriptions:
        is_valid=False
        
        for key in sub.keys():
            if recherche == sub[key]:
                is_valid=True
            
        if is_valid:
            listeRecherche.append(sub)
    #print(len(listeRecherche))        
    return listeRecherche 



def get_infos_location_legacy(subscriptions,subscriptionId,search):
    
    if search=="Unassigned" or search=="Unknown" or "Zone" in search or search=="Intercontinental" or len(search)==0:
        return None
    
    else:
        locations=[]
        for sub in subscriptions:
            if sub["subscriptionId"]==subscriptionId:
                locations = sub["locations"]
                break
        
        recherche=search.replace("AP Southeast","(Asia Pacific) Southeast Asia").replace("AP East","(Asia Pacific) East Asia")
        recherche=recherche.lower().replace(" ","")
        #print(recherche)
        results=[]
        results2=[]
        results3=[]
        
        
        for loc in locations:
            if loc["id"].lower().replace(" ","")==recherche or loc["subscriptionName"].lower().replace(" ","")==recherche or loc["name"].lower().replace(" ","")==recherche or recherche in loc["regionalDisplayName"].lower().replace(" ","") :
                results.append(loc)
            
        if len(results)==0:
            splittedSearch = search.split(" ")
            if len(splittedSearch)==2:
                recherche=search.split(" ")[1]+ " "+search.split(" ")[0]
            elif len(splittedSearch)==3:
                if search.split(" ")[2].isdigit():
                    recherche=search.split(" ")[1]+ " "+search.split(" ")[0]+ " "+search.split(" ")[2]
                else:
                    recherche=search.split(" ")[1]+ " "+search.split(" ")[2]+ " "+search.split(" ")[0]
            else:
                recherche=search.split(" ")[0]
            
            recherche=recherche.replace("AP Southeast","(Asia Pacific) Southeast Asia").replace("AP East","(Asia Pacific) East Asia")
            recherche=recherche.lower().replace(" ","")

            for loc in locations:
                if loc["id"].lower().replace(" ","")==recherche or loc["subscriptionName"].lower().replace(" ","")==recherche or loc["name"].lower().replace(" ","")==recherche or recherche in loc["regionalDisplayName"].lower().replace(" ","") :
                    results.append(loc)
                    
                    
        if len(results)==0:
            splittedSearch = search.replace("JA ","Japan ").split(" ")
            recherche=pycountry.countries.search_fuzzy(splittedSearch[0])[0].name+" "+splittedSearch[1]
            
            recherche=recherche.replace("AP Southeast","(Asia Pacific) Southeast Asia").replace("AP East","(Asia Pacific) East Asia")
            recherche=recherche.lower().replace(" ","")
    
            for loc in locations:
                if loc["id"].lower().replace(" ","")==recherche or loc["displayName"].lower().replace(" ","")==recherche or loc["name"].lower().replace(" ","")==recherche or recherche in loc["regionalDisplayName"].lower().replace(" ","") :
                    results.append(loc)
                    
    
        if len(results)>1:
            #print("Filtre sur name : "+recherche)
            for loc2 in results:
                if loc2["name"]==recherche or loc2["displayName"]==recherche:
                    results2.append(loc2)
            #results=results2  
        
        if len(results)>1:
            #print("Filtre sur recommended : "+search)
            for loc3 in results:
                if loc3["metadata"]["regionCategory"]=="Recommended":
                    results3.append(loc3)
            results=results3
            
        if len(results3)==1:
            results=results3
        if len(results2)==1:
            results=results2
        
        
        if len(results)==1:
            #print("Unique résultat : " + search + " => " + results[0]["name"])
            return results[0]
        elif len(results)==0:
            print("Aucune région trouvée : "+search + " | "+recherche)
            #print() 
            #pass
        else:
            print("Plusieurs régions trouvées : "+ search + " => " + ", ".join([res["name"] for res in results ]))
            #print()
    return (results)


def get_infos_resource(service,resourceId,resourceType):
        
    if len(resourceType.replace("microsoft.","").split("/"))>1:
        rType=resourceType.lower().replace("microsoft.","").split("/")[1].capitalize()
    else:
        rType=resourceType.lower().replace("microsoft.","").split("/")[0].capitalize()
    
    infos={
        "ResourceId":resourceId,
        "ResourceName":resourceId.split("/")[-1].capitalize(),
        "ResourceType":rType,
        "ServiceName":service,
        "ServiceType":resourceType.lower().replace("microsoft.","").split("/")[0].capitalize(), #TODO Compute, Storage, Network, Security, Backup...
        "ServiceInfraType":None #TODO IaaS, SaaS, PaaS
    }
    return infos


# %%
# testPrices["SkuNameFormatted"]=testPrices["SkuName"].apply(lambda row:row.replace("Virtual Machines","").replace(" ","").upper())
# testDataframe["SkuNameFormatted"]=testDataframe["MeterName"]
testPrices=azure_prices.copy()
testPrices["Date"]=pd.to_datetime(testPrices["Date"]).dt.to_period('M').dt.start_time
testPrices["Date"]=testPrices["Date"].apply(lambda x: x.strftime('%Y-%m-%d'))
testPrices["SkuNameFormatted"]=testPrices["SkuName"].apply(lambda row:row.replace("Virtual Machines","").replace(" ","").upper())+testPrices["MeterName"].apply(lambda row:row.replace(" ","").upper())

dataframe=resourcesAzure.copy()
testDataframe=dataframe[dataframe["ServiceName"]=="Virtual Machines"]
# testDataframe["SkuNameFormatted"]=testDataframe["ServiceTier"].apply(lambda row:row.replace("VM","").replace(" ","").upper())
# testDataframe["Location"]=testDataframe["LocationName"]
testDataframe["SkuNameFormatted"]=testDataframe["ServiceTier"].apply(lambda row:row.replace("VM","").replace(" ","").upper())+testDataframe["ResourceSkuSize"].apply(lambda row:row.replace(" ","").upper())

testDataframe=testDataframe.rename(columns={"LocationName":"Location","ServiceName":"Service"})
merge=pd.merge(testDataframe,testPrices,on=["Location","SkuNameFormatted","Service"])
merge.fillna(0)
merge=merge.rename(columns={"OnDemand":"PriceOnDemand","Reservation1Y":"PriceReservation1Y","Reservation3Y":"PriceReservation3Y"})
merge["Calc"]=merge["PriceOnDemand"]*merge["UsageOnDemand"]
merge["Calc2"]=merge["CostOnDemand"]/merge["UsageOnDemand"]
merge["CheckCalc"]=merge["Calc"]==merge["CostOnDemand"]
print(len(merge[merge["CheckCalc"]==False]))
merge[merge["CheckCalc"]==False]

# %%
def get_azure_prices_service_legacy(service, location=None,currency="CHF"):
    # Paramètres de requête pour filtrer les tarifs par région, service et devise
    
    # url = f"https://prices.azure.com/api/retail/prices?currencyCode='{currency}'&$filter=serviceName eq '{service}' and skuName eq 'P2 v3' and reservationTerm eq '1 Year' and unitOfMeasure eq '1 Hour'"
    
    url = f"https://prices.azure.com/api/retail/prices?currencyCode='{currency}'&$filter=serviceName eq '{service}'"

       
    if not location==None:
        url+=f" and armRegionName eq '{location}'"   
       
       
    counter=1
    listAzurePrices=[]
    
    while not url is None and len(url)>0 :
        # Envoi de la requête GET à l'API de tarification Azure
        response = doRequest("GET",url,"")

        # Récupération du prix à la demande en CHF
        data = response.json()        
        
        if len(data)==0 or len(data["Items"])==0:
            print("Erreur de requête, aucun résultat")

        if data and "Items" in data:
            items = data["Items"]
            for item in items: 
                price=None
                inList=False
                indice=-1
                # print(item["retailPrice"])
                last_prices={
                    "OnDemand":{},
                    "Reservation1Y":{},
                    "Reservation3Y":{},
                    "DevTest":{}
                    }
      
                # return item
                # if os=="" or ((os=="Linux" and "Linux" in item["productName"]) or (os == "Windows" and not "Linux" in item["productName"])):

                for i in range(len(listAzurePrices)):
                    p=listAzurePrices[i]
                    
                    if p["Date"]==item["effectiveStartDate"].replace("T00:00:00Z","") and p["RegionName"].lower()==item["armRegionName"].lower() and p["MeterName"].lower()==item["meterName"].lower() and p["Service"].lower()==item["serviceName"].lower() and p["SkuName"].lower()==item["productName"].lower() and p["SkuSize"].lower()==item["skuName"].lower():
                            price=p
                            inList=True
                            indice=i
                            break
                        

                        
                if price==None and not inList:
                    print(item)
                    break
                    price={
                        "Date":item["effectiveStartDate"].replace("T00:00:00Z",""),
                        "Location":item["location"],
                        "Service":item["serviceName"],
                        "RegionName":item["armRegionName"],
                        "SkuName":item["productName"],
                        "MeterName":item["meterName"],
                        "SkuSize":item["skuName"],
                        "OnDemand":None,
                        "Reservation1Y":None,
                        "Reservation3Y":None,
                        "DevTest":None
                    }
                    
                # return items
                    
                if item["type"]=="Reservation":
                    if item["reservationTerm"]=="1 Year":
                        duree =365*24
                        price["Reservation1Y"]=math.ceil(item["retailPrice"]/duree*1000)/1000
                    elif item["reservationTerm"]=="3 Years":
                        duree=3*365*24
                        price["Reservation3Y"]=math.ceil(item["retailPrice"]/duree*1000)/1000
                        
                elif item["type"]=="DevTestConsumption":
                    duree=1
                    price["DevTest"]=math.ceil(item["retailPrice"]*1000)/1000
                else:
                    duree=1
                    price["OnDemand"]=math.ceil(item["retailPrice"]*1000)/1000
                    
                    
                if inList:
                    listAzurePrices[indice]=price
                    if not listAzurePrices[indice]["OnDemand"]==None and not  listAzurePrices[indice]["Reservation1Y"]==None and listAzurePrices[indice]["OnDemand"]<listAzurePrices[indice]["Reservation1Y"]:
                        print(price["SkuSize"],listAzurePrices[indice]["OnDemand"],listAzurePrices[indice]["Reservation1Y"])
                        print(item)
                else:
                    listAzurePrices.append(price)
                    
            print(counter," : ",url)
            counter+=1
            url=data["NextPageLink"]   

    
    print("Requête prix Azure complète : ", service, "=>",location)
    return pd.DataFrame.from_dict(listAzurePrices)
# print(get_azure_prices_service("Azure App Service",location="uksouth"))


def get_azure_prices(services):
    liste_prices=None
    
    for service in services:
        locations=resourcesAzure[resourcesAzure["ServiceName"]==service]["LocationId"].apply(lambda row:row.split("/")[-1]).unique()

        print(service,locations)
        for location in locations:
            prices=get_azure_prices_service(service, location=location,currency="CHF")
            
            if type(liste_prices)!=pd.core.frame.DataFrame :
                liste_prices=prices
            else:
                liste_prices=pd.concat([liste_prices,prices],ignore_index=True)
                
    return liste_prices
        
# azure_prices_sql=get_azure_prices(["SQL Database"])
# %store azure_prices_sql
# result = get_azure_prices_service("Virtual Machines",location="westeurope", currency="CHF")

# %%
def get_gains_reservation_legacy(resourcesAzure,azure_prices):
    
    prices=azure_prices.copy() #Liste 1
    prices["DateStart"]=pd.to_datetime(prices["Date"]).dt.to_period('M').dt.start_time
    # testPrices["Date"]=testPrices["Date"].apply(lambda x: x.strftime('%Y-%m-%d'))
    # prices["SkuNameFormatted"]=prices["SkuName"].apply(lambda row:row.replace("Virtual Machines","").replace(" ","").upper())+prices["MeterName"].apply(lambda row:row.replace(" ","").upper())
    prices=azure_prices[azure_prices["MeterName"]=="P2 v3 App"]
    dataframe=resourcesAzure.copy() #Liste 2
    
    dataframe=dataframe[dataframe["ServiceName"]=="Azure App Service"]
    
    # testDataframe["SkuNameFormatted"]=testDataframe["ServiceTier"].apply(lambda row:row.replace("VM","").replace(" ","").upper())
    # testDataframe["Location"]=testDataframe["LocationName"]
    # dataframe["SkuNameFormatted"]=dataframe["ServiceTier"].apply(lambda row:row.replace("VM","").replace(" ","").upper())+dataframe["ResourceSkuSize"].apply(lambda row:row.replace(" ","").upper())
    # return dataframe
    dataframe=dataframe.rename(columns={"LocationName":"Location","ServiceName":"Service"})
    merge=pd.merge(dataframe,prices,on=["Location","SkuNameFormatted","Service"])
    
    
    filtered = merge[merge["DateStart"]<merge["Date_x"]]
    
    filtered=filtered.sort_values("DateStart")
    grouped = filtered.groupby(['Service', 'Location', 'RegionName', 'SkuName', 'MeterName', 'SkuSize'])
    
    # print(grouped["OnDemand"])
    # # Remplacer les valeurs nulles avec les valeurs non nulles correspondantes dans chaque groupe
    filtered['OnDemand'] = grouped['OnDemand'].fillna(method='ffill')
    filtered['Reservation1Y'] = grouped['Reservation1Y'].fillna(method='ffill')
    filtered['Reservation3Y'] = grouped['Reservation3Y'].fillna(method='ffill')
    filtered['DevTest'] = grouped['DevTest'].fillna(method='ffill')

    # filtered=filtered.sort_values("DateStart")

    filtered=filtered.rename(columns={"Date_x":"Date","OnDemand":"PriceOnDemand","Reservation1Y":"PriceReservation1Y","Reservation3Y":"PriceReservation3Y"})
    

    result = filtered.groupby(['Date', "ProjectName",'LocationId',"Location","Service","ServiceTier","ResourceName","ResourceId","ResourcePath","SkuName","SkuSize","ResourceSkuTier","ResourceSkuSize","SkuNameFormatted","MeterName","DateStart","UsageTot","UsageOnDemand","UsageReservation1Y","UsageReservation3Y","CostTot","CostOnDemand","CostReservation1Y","CostReservation3Y","PriceOnDemand","PriceReservation1Y","PriceReservation3Y"], as_index=False)['DateStart'].max()
    result=result.rename(columns={"DateStart":"DatePriceStart"})
    result=result.fillna(0)
    
    result["UnitCostOnDemand"] = result[["CostOnDemand", "UsageOnDemand"]].apply(lambda row: row["CostOnDemand"] / row["UsageOnDemand"] if row["UsageOnDemand"] != 0 else 0, axis=1)
    result["UnitCostReservation1Y"] = result[["CostReservation1Y", "UsageReservation1Y"]].apply(lambda row: row["CostReservation1Y"] / row["UsageReservation1Y"] if row["UsageReservation1Y"] != 0 else 0, axis=1)
    
    # result["UnitCostReservation3Y"]=result["CostReservation3Y"]/result["UsageReservation3Y"]
    
    # data["UnitCostOnDemand"]=data.apply(lambda ligne: data[(data["ResourceName"]==ligne["ResourceName"])&(data["ResourceSkuSize"]==ligne["ResourceSkuSize"])&(data["CostOnDemand"]>0)]["UnitCostOnDemand"].mean() if not type(ligne["UnitCostOnDemand"])==float and ligne["UnitCostOnDemand"].isna() else ligne["UnitCostOnDemand"],axis=1)

    result["GainReservation"]=result.apply(lambda row: (row["UnitCostOnDemand"]- row["UnitCostReservation1Y"])*row["UsageReservation1Y"] if row["UnitCostOnDemand"] >= 0 else (row["PriceOnDemand"]- row["UnitCostReservation1Y"])*row["UsageReservation1Y"], axis=1)
    
    # result["CalcRes"]= result["CoutHoraireReelReservation1Y"] /result["PriceReservation1Y"]
    # result["CalcOn"]= result["CoutHoraireReelOnDemand"] /result["PriceOnDemand"]
    
    # result["GainReservation"]=result["UsageReservation1Y"]*(result["PriceOnDemand"]-result["PriceReservation1Y"])+result["UsageReservation3Y"]*(result["PriceOnDemand"]-result["PriceReservation3Y"])

    # result["CheckCalc"]=abs(result["CoutHoraireReelOnDemand"]-result["PriceOnDemand"])/result["PriceOnDemand"]<0.1
    # return result[(result["CheckCalc"]==False)]
    return result
    
gains_reservation_test=get_gains_reservation_legacy(resourcesAzure.copy(),azure_prices.copy())
gains_reservation_test
# lignes_gains_reservation=lignes_gains_reservation.sort_values(["ResourceId","Date"])
# pd.set_option('display.max_rows', None)
# (lignes_gains_reservation[["Date","Service","ResourceId","Location","SkuSize","PriceOnDemand","PriceReservation1Y","CalcOn","CalcRes"]])

# %%
def get_azure_prices_service(service, location=None,currency="CHF"):
    # Paramètres de requête pour filtrer les tarifs par région, service et devise

    url = f"https://prices.azure.com/api/retail/prices?currencyCode='{currency}'&$filter=serviceName eq '{service}'"
       
       
    if not location==None:
        url+=f" and location eq '{location}'"   
       
    # url+=" and productId eq '1014876'"
    counter=1
    listAzurePrices=[]
    
    while not url is None and len(url)>0 :
        # Envoi de la requête GET à l'API de tarification Azure
        response = doRequest("GET",url,"")

        # Récupération du prix à la demande en CHF
        data = response.json()        

        if len(data)==0:
            print("Erreur de requête, aucun résultat")
        
        if data and "Items" in data:
            items = data["Items"]
            for item in items: 
                price=None
                inList=False
                indice=-1

                return item
                # return item
                # if os=="" or ((os=="Linux" and "Linux" in item["productName"]) or (os == "Windows" and not "Linux" in item["productName"])):

                for i in range(len(listAzurePrices)):
                    p=listAzurePrices[i]
                    
                    if p["Date"]==p["effectiveStartDate"].replace("T00:00:00Z","") and p["RegionName"].lower()==item["armRegionName"].lower() and p["MeterName"].lower()==item["meterName"].lower() and p["ServiceName"].lower()==item["serviceName"].lower() and p["SkuName"].lower()==item["productName"].lower() and p["SkuSize"].lower()==item["skuName"].lower():
                        price=p
                        inList=True
                        indice=i
                        break
                        
                if price==None and not inList:
                    price={
                        "Date":item["effectiveStartDate"].replace("T00:00:00Z",""),
                        "LocationName":item["location"],
                        "ServiceName":item["serviceName"],
                        "RegionName":item["armRegionName"],
                        "SkuName":item["productName"],
                        "MeterName":item["meterName"],
                        "SkuSize":item["skuName"],
                        "OnDemand":None,
                        "Reservation1Y":None,
                        "Reservation3Y":None,
                        "DevTest":None
                    }
                    
                if item["type"]=="Reservation":
                    if item["reservationTerm"]=="1 Year":
                        duree =365*24
                        price["Reservation1Y"]=math.ceil(item["retailPrice"]/duree*1000)/1000
                    elif item["reservationTerm"]=="3 Years":
                        duree=3*365*24
                        price["Reservation3Y"]=math.ceil(item["retailPrice"]/duree*1000)/1000
                        
                elif item["type"]=="DevTestConsumption":
                    duree=1
                    price["DevTest"]=math.ceil(item["retailPrice"]*1000)/1000
                else:
                    duree=1
                    price["OnDemand"]=math.ceil(item["retailPrice"]*1000)/1000
                    
                    
                if inList:
                    listAzurePrices[indice]=price
                    if not listAzurePrices[indice]["OnDemand"]==None and not  listAzurePrices[indice]["Reservation1Y"]==None and listAzurePrices[indice]["OnDemand"]<listAzurePrices[indice]["Reservation1Y"]:
                        print(price["SkuSize"],listAzurePrices[indice]["OnDemand"],listAzurePrices[indice]["Reservation1Y"])
                        print(item)
                else:
                    listAzurePrices.append(price)
                    
            print(counter," : ",url)
            counter+=1
            url=data["NextPageLink"]   

            
    print("Requête prix Azure complète : ", service)
    return pd.DataFrame.from_dict(listAzurePrices)



def get_azure_prices(services):
    liste_prices=None
    
    for service in services:
        locations=resourcesAzure[resourcesAzure["ServiceName"]=="Azure App Service"]["LocationName"].unique()
        print(service,locations)
        for location in locations:
            prices=get_azure_prices_service(service, location=location,currency="CHF")
            
            if type(liste_prices)!=pd.core.frame.DataFrame :
                liste_prices=prices
            else:
                liste_prices=pd.concat([liste_prices,prices],ignore_index=True)
                
    return liste_prices
        
# azure_prices=get_azure_prices(["Azure App Service","Virtual Machines","Redis Cache"])
# %store azure_prices
# get_azure_prices_service("Azure App Service", currency="CHF")

get_azure_prices_service("Virtual Machines")


# %%
def get_gains_tot_legacy(data,reservation=False,rightsizing=True):
    # data = resourcesAzure.copy()
    # data=data[data["ServiceName"]=="Azure App Service"]
    

    # #RIGHTSIZING
    # data["ResourceConfig"] = data.apply(lambda ligne: get_config(ligne), axis=1)
        
    
    # data["UnitCostOnDemand"]=data.apply(lambda l : l["CostOnDemand"]/l["UsageOnDemand"] if l["UsageOnDemand"]>0 and l["CostOnDemand"]>0 else 0,axis=1)
    # data["UnitCostOnDemand"]=data.apply(lambda ligne: get_retail_unit_cost(data,ligne),axis=1)
    # print("Traitement terminé : Pré-traitement")
    
    #RESERVATION 
    if reservation:
        data["UnitCostReservation1Y"]=data.apply(lambda l : l["CostReservation1Y"]/l["UsageReservation1Y"] if l["UsageReservation1Y"]>0 and l["CostReservation1Y"]>0 else 0,axis=1)
        data["UnitCostReservation3Y"]=data.apply(lambda l : l["CostReservation3Y"]/l["UsageReservation3Y"] if l["UsageReservation3Y"]>0 and l["CostReservation3Y"]>0 else 0,axis=1)
        data["GainReservation"]=data.apply(lambda ligne:(ligne["UnitCostOnDemand"]-ligne["UnitCostReservation1Y"])*ligne["UsageReservation1Y"]+(ligne["UnitCostOnDemand"]-ligne["UnitCostReservation3Y"])*ligne["UsageReservation3Y"] if ligne["CostTot"]>0 and ligne["UnitCostOnDemand"]>0 else 0,axis=1)
        print("Traitement terminé : Réservation")
    

    
    data_rightsizing=data.groupby(["Date","ProjectName","ApplicationName","ServiceName","LocationName","LocationId","ResourceName","ResourcePath","ResourceConfig"])[["UsageTot","CostTot","UsageOnDemand","CostOnDemand"]].sum().reset_index()


    
    data_rightsizing["ResourceSkuSize"] = data_rightsizing.apply(lambda row: list(data[
        (data["Date"] == row["Date"]) &
        (data["ResourceName"] == row["ResourceName"]) &
        (data["ResourceConfig"] ==row["ResourceConfig"]   )    ]["ResourceSkuSize"].unique()), axis=1)
    
    data_rightsizing=data_rightsizing.sort_values(["ResourcePath","Date"])
    data_rightsizing["GainRightSizing"] = data_rightsizing.apply(lambda row: 0.0, axis=1)

    counter_rightsizing={}
        
    for i in range(len(data_rightsizing)-2):
        row1=data_rightsizing.iloc[i]
        row2=data_rightsizing.iloc[i+1]
        row3=data_rightsizing.iloc[i+2]
            
        if row1["ResourceName"]==row2["ResourceName"] and row2["ResourceName"]==row3["ResourceName"] and row1["ResourceConfig"]==row2["ResourceConfig"] and row2["ResourceConfig"]==row3["ResourceConfig"] and row1["LocationName"]==row2["LocationName"] and row2["LocationName"]==row3["LocationName"] and (sorted(row1["ResourceSkuSize"])!=sorted(row2["ResourceSkuSize"]) or len(row1["ResourceSkuSize"])!=len(row2["ResourceSkuSize"])) and (sorted(row2["ResourceSkuSize"])!=sorted(row3["ResourceSkuSize"]) or len(row2["ResourceSkuSize"])!=len(row3["ResourceSkuSize"])):
            print(row1["ResourceSkuSize"],row3["ResourceSkuSize"])
            
            if 'P2 v2 App' == row1["ResourceSkuSize"] and 'P2 v3 App' == row3["ResourceSkuSize"]:
                valeur = "P2 v2 => P2 v3"
                print(valeur)
                #Changement vers SKU cible P2 v3
                
                
            elif 'P3 v2 App' == row1["ResourceSkuSize"] and 'P2 v3 App' == row3["ResourceSkuSize"]:
                valeur = "P3 v2 => P2 v3"
                print(valeur,row1["ResourceSkuSize"],row3["ResourceSkuSize"])
                #Changement vers SKU cible P2 v3
                
            else:
                valeur="Autre"
                print(row1["ResourceName"],row1["ResourceSkuSize"],row3["ResourceSkuSize"])
            
            if not row2["Date"] in counter_rightsizing:
                counter_rightsizing[row2["Date"]]={}
                
            if not valeur in counter_rightsizing[row2["Date"]]:
                counter_rightsizing[row2["Date"]][valeur]=1
            else:
                counter_rightsizing[row2["Date"]][valeur]=counter_rightsizing[row2["Date"]][valeur]+1
            
            data_rightsizing.loc[(data_rightsizing["ResourceName"] == row1["ResourceName"]) & (data_rightsizing["ResourceConfig"] == row1["ResourceConfig"]), "GainRightSizing"] = data_rightsizing[(data_rightsizing["ResourceName"] == row1["ResourceName"]) & (data_rightsizing["ResourceConfig"] == row1["ResourceConfig"])].apply(lambda row: (row["UnitCostOnDemand"] - row1["UnitCostOnDemand"]) * row["UsageOnDemand"] if row["UnitCostOnDemand"] > 0 and row1["UnitCostOnDemand"] > 0 and row["Date"] > row1["Date"] else 0, axis=1)

                
        if i%100==0:
            print("Avancement :","{:.0%}".format((i+1)/(len(data_rightsizing)-2)))
    
    # (data["UnitCostOnDemand"]-data["UnitCostReservation1Y"])*data["UsageReservation1Y"]+(data["UnitCostOnDemand"]-data["UnitCostReservation3Y"])*data["UsageReservation3Y"]
    print("Traitement terminé : Rightsizing")
    return data
# get_gains_tot(data,reservation=False,rightsizing=True)
# get_gains_tot(resourcesAzure,reservation=False,rightsizing=True)
# gains_reservation=get_gains_reservation(resourcesAzure)
# %store gains_reservation

# %%
def list_filter(liste, filters,equal=False):
    listeFiltree = []
    for obj in liste:
        is_valid = True
        for attr, value in filters:
            if equal:
                condition=not value.lower() == obj[attr].lower()
            else:
            
                condition=not value.lower() in obj[attr].lower()
            if not attr in obj.keys() or (condition and not value is None):
                is_valid = False
                break
        if is_valid:
            listeFiltree.append(obj)
    return listeFiltree

def select_output(liste,output,unique):
    listeOutputs=[]
    for obj in liste:
        is_valid=True
        if not output in obj.keys() or obj[output] in listeOutputs:
            is_valid=False
        if is_valid or not unique:
            listeOutputs.append(obj[output])
    return listeOutputs

def select_outputs(liste, outputs, unique=False):
    listeOutputs = []
    
    for obj in liste:
        item = {}
        for out in outputs:
            if out in obj.keys():
                item[out] = obj[out]
        
        if unique:
            inList=False
            for elt in listeOutputs:
                
                
                for key in elt.keys():
                    allKeys=True
                    # print(key,elt[key],item[key])
                    if elt[key]!=item[key]:
                        allKeys=False
                        break
                if allKeys:
                    inList=True
                                
            if not inList:
                listeOutputs.append(item)
           
        else:
            listeOutputs.append(item)
    
    return listeOutputs

def sum_outputs(liste,outputs):
    dates = select_output(liste,"Date",True)
    
    sums={}
    for d in dates : 
        sums[d]={}
        for out in outputs:
            sums[d][out]=0
    

    for obj in liste:
        for out in outputs:
            if out in obj.keys() :
               sums[obj["Date"]][out]+=float(obj[out] )
    return sums

def list_to_dataframe(input_list):

    # Create a dictionary from the input list
    data = {}
    for index, item in enumerate(input_list):
        data[index] = item

    # Convert the dictionary to a pandas DataFrame
    df = pd.DataFrame.from_dict(data, orient='index')
    df = df.applymap(lambda x: math.ceil(x * 1000) / 1000 if isinstance(x, float) and not math.isnan(x) else x)
    df.style.apply(['font-weight: bold'], axis=1)

    return df

def nombre_jours_mois(date_str):
    year, month, day = map(int, date_str.split('-'))
    d = date(year, month, day)
    if month == 12:
        next_month = date(year + 1, 1, day)
    else:
        next_month = date(year, month + 1, day)
    return (next_month - d).days

# list_to_dataframe(select_outputs(listeAppService,["ServicePlanName","SkuTier","SkuSize"],True))

#list_to_dataframe(select_outputs(list_filter(lignesCost, [("Service","Azure App Service"),("PricingModel","Reservation")]),["Subscription","ResourceGroup","Service","Cost","Cost journalier","PricingModel","Reservation"]))

# %% [markdown]
# Management hierarchie

# %%
def get_management_hierarchy(relations,mgId):
    #print(mgId,mgName)
    #return data["properties"]["children"]
    if not mgId in relations.keys():
        url=f"https://management.azure.com/providers/Microsoft.Management/managementGroups/{mgId}?api-version=2020-05-01&%24expand=children"
    
        response=doRequest("GET",url,"")
        
        data=response.json()

        #print(data)
        if data==None:
            print(response.text)
        #print(data["properties"]["children"])
        
        if not data["properties"]["children"]==None:
            children=[(child["type"].split("/")[1],child["name"],child["displayName"]) for child in data["properties"]["children"]]

            relations[mgId]=children
            for child in children:
                #print(child)
                if child[0]=="managementGroups":
                    relations=get_management_hierarchy(relations,child[1])

    
    return(relations)

# managementHierarchyAzure=get_management_hierarchy({},"mg-rolex")
# %store managementHierarchyAzure

# %% [markdown]
# Tagging

# %%
def get_resource_tags_subscription(subscription_id):

    # Initialize URL for resources API
    url = f"https://management.azure.com/subscriptions/{subscription_id}/tagNames?api-version=2021-04-01"

    while len(url)>0:
        # Get all resources in the subscription
        response = doRequest("GET",url,"")
        data = response.json()["value"]

        tags=[]

        for d in data:
        
            tag={
                "Subscription":d["id"].split("/")[2],
                "Tag name":d["tagName"],
            # "Calcul":d["count"]["type"],
                "Nombre":d["count"]["value"],
                "Valeurs":[(v["tagValue"], v["count"]["value"]) for v in d["values"]]
            }
            
            tags.append(tag)
        if "nextLink" in response.json().keys():
            url =response.json()["nextLink"]
            print(subscription_id,":",url)
        else:
            url=""
    return tags
  
def get_resource_tags_all_subscriptions():
    
    tags=[]
    for sub in subscriptions:
        tags+=get_resource_tags_subscription(sub["subscriptionId"])
        
    return tags

#config = get_resource_sku("6c79c65b-cbbd-4260-803d-095edc94b456","rg-xcs-ndef-test","wa-ndef-kmsclt-chn-tst")
listeTags=get_resource_tags_all_subscriptions()

# %%
def get_resource_tags_subscription(subscription_id):

    # Initialize URL for resources API
    url = f"https://management.azure.com/subscriptions/{subscription_id}/tagNames?api-version=2021-04-01"

    while len(url)>0:
        # Get all resources in the subscription
        response = doRequest("GET",url,"")
        data = response.json()["value"]

        tags=[]

        for d in data:
        
            tag={
                "Subscription":d["id"].split("/")[2],
                "Tag name":d["tagName"],
            # "Calcul":d["count"]["type"],
                "Nombre":d["count"]["value"],
                "Valeurs":[(v["tagValue"], v["count"]["value"]) for v in d["values"]]
            }
            
            tags.append(tag)
        if "nextLink" in response.json().keys():
            url =response.json()["nextLink"]
            print(subscription_id,":",url)
        else:
            url=""
    return tags
  
def get_resource_tags_all_subscriptions():
    
    tags=[]
    for sub in subscriptions:
        tags+=get_resource_tags_subscription(sub["subscriptionId"])
        
    return tags

#config = get_resource_sku("6c79c65b-cbbd-4260-803d-095edc94b456","rg-xcs-ndef-test","wa-ndef-kmsclt-chn-tst")
listeTags=get_resource_tags_all_subscriptions()

def sum_tags_values(tags_values):
    valeurs=[]
    for ligne in tags_values:
        #print(lSigne)
        for tagValue,count in ligne:
            if tagValue=="":
                tagValue="Null"
        # print(tagValue+ " => " +str(count))
            inList=False
            for valeur in valeurs:
                if valeur["Tag value"]==tagValue:
                    if tagValue=="Null":
                        valeur["CountNull"]+=count
                        # print(tagValue)
                    else:
                        valeur["Count"]+=count
                    inList=True
            
            if not inList:
                if tagValue=="Null":
                    valeurs.append({
                        "Tag value":tagValue,
                        "Count":0,
                        "CountNull":count
                    })
                else:
                    valeurs.append({
                        "Tag value":tagValue,
                        "Count":count,
                        "CountNull":0
                    })            
    return valeurs

def get_ressource_eligibles_tags():
    services_eligibles=["Azure Front Door Service","Logic Apps","Azure Data Factory v2","Azure Synapse Analytics","ExpressRoute","Azure DevOps","Azure App Service","Virtual Machines",
        "Functions","Azure DNS",'SQL Database', 'Virtual Network', 'Backup', 'Key Vault',"API Management","Service Bus","Event Hubs","Load Balancer","IoT Hub",
        "Container Registry","Redis Cache","Azure Database for PostgreSQL", 'Azure Cognitive Search','Azure Database for MariaDB',"Container Instances","Azure Bastion",
        "Application Gateway","Cognitive Services","Network Watcher","Automation","NAT Gateway","VPN Gateway","Event Grid","Azure DDOS Protection"]

    counter={}
    
    dates=sorted(resourcesAzure["Date"].unique())

    for sub in subscriptions:
        data=resourcesAzure.copy()
        data=data[(data["ProjectId"]==sub["subscriptionId"])&(data["Date"]==dates[-1])]
        
        resources=[]
        resourcesVM=[]
        
        for i in range(len(data)):
            # print(data["ServiceName"].iloc[i])
            if data["ServiceName"].iloc[i] in services_eligibles:
                resources.append(data["ResourceName"].iloc[i])
                
                
        # return pd.DataFrame(resources).empty
        if pd.DataFrame(resources).empty:
            counter[sub["subscriptionId"]]=0
        else:
            counter[sub["subscriptionId"]]= len(pd.DataFrame(resources)[0].unique())
        # return data
        
        # for l in data:
            # print(l)
        # len(resourcesAzure[(resourcesAzure["ProjectId"]==sub["subscriptionId"])&(resourcesAzure["Date"]=="2023-06-01")]["ResourceName"].unique()
    
    return counter
    
    
def show_tagging_state(listeTags):
    resourcesEligiblesSubscriptionsTags= get_ressource_eligibles_tags()
    count=0
    for sub in resourcesEligiblesSubscriptionsTags.keys():
        # print(sub, resourcesEligiblesSubscriptionsTags[sub])
        count+=resourcesEligiblesSubscriptionsTags[sub]
        # print(count)
    
    print("Resources éligibles au tagging récupérées :",count,"ressources éligibles.")
    
    data = {}
    for index, item in enumerate(listeTags):
        data[index] = item

    # Convert the dictionary to a pandas DataFrame
    df = pd.DataFrame.from_dict(data, orient='index')
    agg_df = pd.DataFrame(columns=['Tag name', 'Subscriptions', 'Nombre', 'Valeurs'])
    
    # Boucler sur les tags
    for tag in df['Tag name'].unique():
       
        # tag=""
        # Filtrer le dataframe par le tag en cours
        filtered_df = df[df['Tag name'] == tag]
        
        # Créer une chaîne de caractères contenant la liste des abonnements correspondant à ce tag
        countResourcesEligibles=0
        liste_subs_names=[]
        
        
        for sub in filtered_df['Subscription'].unique():
            countResourcesEligibles+=resourcesEligiblesSubscriptionsTags[sub]
            if not resourcesAzure[resourcesAzure["ProjectId"]==sub].empty:
                projectName=resourcesAzure[resourcesAzure["ProjectId"]==sub]["ProjectName"].iloc[0]
            else:
                projectName=sub
            liste_subs_names.append(projectName)
        
        subscriptions = ', '.join(liste_subs_names)
        
        # Calculer la somme des nombres pour ce tag
        nombre = filtered_df['Nombre'].sum()
        
        # Créer une chaîne de caractères contenant la liste des valeurs correspondant à ce tag
        # sum_tags_values = sum_tags_values(filtered_df['Valeurs'].values))
        
        # print(filtered_df['Valeurs'].values)
        valeurs=(sum_tags_values(filtered_df['Valeurs'].values))
        
        nombreNull=0
        for val in valeurs:
            nombreNull+=int(val["CountNull"])
        # print(valeurs)
        # break 
        # Ajouter une ligne au dataframe agrégé avec les informations agrégées pour ce tag
        agg_df = agg_df.append({'Tag name': tag, 'Subscriptions': subscriptions,"Resources Eligibles":str(int(countResourcesEligibles)), 'Nombre': nombre,'Nombre Null': str(int(nombreNull)),  'Valeurs': ", ".join([v["Tag value"]+ " ("+str(v["Count"]+v["CountNull"])+")" for v in valeurs])}, ignore_index=True)
    return agg_df


tagging_state = show_tagging_state(listeTags)
date =datetime.now()
date_format = date.strftime("%Y%m%d")

tagging_state.to_csv(f"C:\\Users\\hugo.dufrene\\Wavestone\\WE - ROLEX [CH] - Support mise en place Gouvernance Cloud public - 13665 - Documents partages\\Gouvernance Cloud\\FinOps\\Optimisations\\Tagging\\{date_format} - Tagging Status.csv",index=False)

# %%
tagging_state

# %%
len(resourcesAzure[(resourcesAzure["ServiceName"]=="Virtual Machines")&(resourcesAzure["Date"]=="2023-07-01")]["ResourceName"].unique())

# %%
services_eligibles=["Azure Front Door Service","Logic Apps","Azure Data Factory v2","Azure Synapse Analytics","ExpressRoute","Azure DevOps","Azure App Service","Virtual Machines",
        "Functions","Azure DNS",'SQL Database', 'Virtual Network', 'Backup', 'Key Vault',"API Management","Service Bus","Event Hubs","Load Balancer","IoT Hub",
        "Container Registry","Redis Cache","Azure Database for PostgreSQL", 'Azure Cognitive Search','Azure Database for MariaDB',"Container Instances","Azure Bastion",
        "Application Gateway","Cognitive Services","Network Watcher","Automation","NAT Gateway","VPN Gateway","Event Grid","Azure DDOS Protection"]
len(services_eligibles)

# %%
get_ressource_eligibles_tags()

# %%
resourcesEligiblesSubscriptionsTags= get_ressource_eligibles_tags()

# %%
count=0
for sub in resourcesEligiblesSubscriptionsTags.keys():
    print(sub, resourcesEligiblesSubscriptionsTags[sub])
    count+=resourcesEligiblesSubscriptionsTags[sub]
    print(count)
    

# %%
resourcesAzure["ServiceName"].unique()
#https://learn.microsoft.com/fr-fr/azure/azure-resource-manager/management/tag-support

#exclus "Storage","Unassigned","Azure Cosmos DB","Log Analytics","Azure Firewall Manager","Advanced Threat Protection",
# "Virtual Machines Licenses","Bandwidth","Security Center","Content Delivery Network","Azure Monitor","Azure Defender","Azure Data Explorer",
"Azure Firewall","Visual Studio Subscription","Insight and Analytics","Advanced Data Security"

#inclus 


# %%
resourcesAzure["Date"].unique()

# %%
resourcesAzure[resourcesAzure["ProjectId"]=="00fd21d5-9fda-4aaa-88f5-1b88f385006c"]["ProjectName"].iloc[0]

# %%
for sub in subscriptions:
    # print(sub["subscriptionId"])
    print(sub["subscriptionId"],sub["subscriptionName"],len(resourcesAzure[(resourcesAzure["ProjectId"]==sub["subscriptionId"])&(resourcesAzure["Date"]=="2023-06-01")]["ResourceName"].unique()))
    

# %%
def set_tagging_policy():
    policy={}
    
    tags=[
        "name",
        "application",
        "resourceFunction",
        "cluster", 
        "environment", #Dev, Prod, Qual, Preprod...
        "version",
        "ownerTeam",
        "ownerProduct",
        "businessUnit",
        "user", #métier, IT, client final...
        "project",
        "criticity", #low, medium, high
        "compliance",
        "autoShutdown", #yes/no
        "autoStartTime", #08:00
        "autoShutTime", #18:00
        "spotEligible",
        "reservationEligible"
    ]    

# %%
def get_app_service_plan(serverFarmId):
    url=f"https://management.azure.com/{serverFarmId}?api-version=2022-03-01"
    response=doRequest("GET",url,"") 
    return response
   
def get_app_service_resources_subscription(subscriptionId):
    url = f"https://management.azure.com/subscriptions/{subscriptionId}/providers/Microsoft.Web/sites?api-version=2022-03-01"

    response=doRequest("GET",url,"")
    data=response.json()["value"]
    
    listeAppService=[]
    for item in data:
        appPlan = get_app_service_plan(item["properties"]["serverFarmId"])
        
        if  not appPlan==None:
            appPlan=appPlan.json()
        
            os="Windows"
            for version in item["properties"]["siteProperties"]["properties"]:
                if "WINDOWS" in version["name"].upper() and not version["value"] is None and  len(version["value"]) >0:
                    os="Windows"
                    
                if "WINDOWS" in version["name"].upper() and not version["value"] is None and len(version["value"]) >0:
                    os="Linux"
            
            if "LINUX" in item["kind"].upper():
                os="Linux" 

            app={
                "Subscription":subscriptionId,
                "Service":"Azure APp Service",
                "ResourceGroup":item["id"].split("/")[4],
                "Location":item["location"],
                "ServerFarmId":item["properties"]["serverFarmId"],
                "ServerFarm":item["properties"]["serverFarmId"].split("/")[-1],
                "ServicePlanName":appPlan["properties"]["planName"],
                "Type":appPlan["kind"],
                "SkuTier":appPlan["sku"]["tier"],
                "SkuSize":appPlan["sku"]["size"],
                "OS":os,
            # "Reserved":item["reserved"],
                "Tags":item.get("tags", {}),
                "ResourceId":item["id"].lower()
            }
            listeAppService.append(app) 
        
    
    return listeAppService

def get_app_service_all_resources():
    listeAppService=[]
    subscriptions=getSubscriptions()
    for i in range(len(subscriptions)):
        result=get_app_service_resources_subscription( subscriptions[i]["subscriptionId"])
        if result==None:
            time.sleep(20)
            result=get_app_service_resources_subscription( subscriptions[i]["subscriptionId"])
        
        if result!=None:
            listeAppService+=result
            print( str(i+1) + " : " + subscriptions[i]["displayName"])
    return listeAppService


def get_app_service_restants_resources(listeAppService):
    subscriptions=getSubscriptions()
    abonnementsEffectues=select_output(listeAppService,"Subscription",True)

    print(abonnementsEffectues)
    for i in range(len(subscriptions)):

        if not subscriptions[i]["subscriptionId"] in abonnementsEffectues:
        
            result=get_app_service_resources_subscription( subscriptions[i]["subscriptionId"])
            if result==None:
                time.sleep(20)
                result=get_app_service_resources_subscription( subscriptions[i]["subscriptionId"])
            
            if result!=None:
                listeAppService+=result
                print( str(i+1) + " : " + subscriptions[i]["displayName"]+" => "+str(len(listeAppService)))
    return listeAppService


def getRessourceInfos(listeAppService,ResourceId):
    results=list_filter(listeAppService,[("ResourceId",ResourceId)])

    if len(results)==1:
        return results[0]
    else:
        for res in results:
            if res["ResourceId"].lower()==ResourceId.lower():
                return res
    #print("Récupération infos SKU impossible. Instance décommissionnée : "+ResourceId)
    return None

#token=getToken()
# listeAppService=get_app_service_all_resources()

# %store listeAppService


# %%
def get_reservation_rate(data,service,serviceTier,journalier):
    if serviceTier=="":
        usageOnDemand = sum_outputs(list_filter(data, [("Service",service),("PricingModel","OnDemand")]),["Usage","Usage journalier"])
        usageReserve = sum_outputs(list_filter(data, [("Service",service),("PricingModel","Reservation")]),["Usage","Usage journalier"])

    else:
        usageOnDemand = sum_outputs(list_filter(data, [("Service",service),("ServiceTier",serviceTier),("PricingModel","OnDemand")]),["Usage","Usage journalier"])
        usageReserve = sum_outputs(list_filter(data, [("Service",service),("ServiceTier",serviceTier),("PricingModel","Reservation")]),["Usage","Usage journalier"])

    
    reservationRate = {}

    dates = sorted(set(usageOnDemand.keys()) | set(usageReserve.keys()))
    donnee = "Usage"

    if journalier:
        donnee += " journalier"

    for d in dates:
        UR = usageReserve.get(d, {donnee: 0}).get(donnee, 0)
        UOD = usageOnDemand.get(d, {donnee: 0}).get(donnee, 0)
        reservationRate[d] = UR / (UR + UOD)
    
    return reservationRate

def get_price_rate(data):
    usages=sum_outputs(data,["Usage","Coût"])
    if len(list(usages.keys()))>0:
        date =list(usages.keys())[0]
        return usages[date]["Coût"]/usages[date]["Usage"]
    else:
        print(usages.keys())
        return None

def get_price_rates(data):
    dates=sorted(select_output(data,"Date",True),reverse=True)

    tarifHoraire={}
    for d in dates:
        usages=sum_outputs(list_filter(data,[("Date",d)]),["Coût","Coût journalier","Usage","Usage journalier"])[d]
        tarifHoraire[d]=usages["Coût"]/usages["Usage"]
    #prices=sum_outputs(data,["Coût","Coût journalier","Usage","Usage journalier"])
    return(tarifHoraire)




def get_price_table(service,listeAppService,appServicePrices):
    sortedAppService = sorted(listeAppService, key=lambda x: ( x["Location"],x["SkuSize"]))
    
    table = []
    #service="Azure App Service"

    #appServicePrices=get_azure_prices_region(service=service,currency="CHF")
        
    for appService in sortedAppService:
        location=appService["Location"]
        
     
        appServicePrice=None
        listeAppServicePrice=[]
        #listeAppServicePrice = list_filter(appServicePrices,[("Location",location),("SkuSize",appService["SkuSize"])],True)
        #if location=="East Asia":             
             #print("1",len(listeAppServicePrice),location)
             
        if len(listeAppServicePrice)==0:
            location=appService["Location"].replace(" ","").lower().strip()
            listeAppServicePrice=list_filter(appServicePrices,[("SkuSize",appService["SkuSize"]),("RegionName",location)],True)
            #print("1",location,len(listeAppServicePrice))     
        
        if len(listeAppServicePrice)==0:
            location=appService["Location"].split(" ")[1]+" "+location.split(" ")[0]
            listeAppServicePrice=list_filter(appServicePrices,[("SkuSize",appService["SkuSize"]),("Location",location)],True)
            print("2",location,len(listeAppServicePrice))   
            
        if len(listeAppServicePrice)==0:
            location=pycountry.countries.search_fuzzy(appService["Location"].split(" ")[0])[0].alpha_2+" "+appService["Location"].split(" ")[1]
            listeAppServicePrice=list_filter(appServicePrices,[("SkuSize",appService["SkuSize"]),("Location",location)],True)
            print("3",location,len(listeAppServicePrice))   
    
        if len(listeAppServicePrice)==0:
            location=appService["Location"].replace("West Europe","EU West").replace("North Europe","EU North")
            listeAppServicePrice=list_filter(appServicePrices,[("SkuSize",appService["SkuSize"]),("Location",location)],True)
            print("4",appService["Location"],location,len(listeAppServicePrice))  
            
        if len(listeAppServicePrice)==0:
            print("Aucun résultat : ",appService["ResourceGroup"],appService["Location"],location.replace(" ","").lower().strip(),appService["SkuSize"])
        
        elif len(listeAppServicePrice)>1:
            
            count=0
            uniquePrice=None
            for price in listeAppServicePrice:
                if ((appService["OS"]=="Linux" and "Linux" in price["SkuName"]) or (appService["OS"]=="Windows" and not "Linux" in price["SkuName"])):
                    count+=1
                    uniquePrice=price
            
            if count==1:
                appServicePrice= uniquePrice
            else:
                appServicePrice=None
                print("Plusieurs résultats 2: ",appService["Location"],appService["SkuSize"])
                print("Résultats : ",appServicePrices)
                break
            
        else:
            appServicePrice=listeAppServicePrice[0]
              
                   
        if not appServicePrice==None :
            tableElement={
                "Nombre":1,
                "Location":appService["Location"],
                "ServiceTier":appServicePrice["SkuName"],
                "Sku":appService["SkuSize"],
                "OS":appService["OS"],
                "On Demand":appServicePrice["On Demand"],
                "1Y Reservation":appServicePrice["1Y Reservation"],
                "3Y Reservation":appServicePrice["3Y Reservation"]
                }
            inList=False
            eltIndex=-1
            for i in range(len(table)):
                elt=table[i]
                if elt["Location"]==tableElement["Location"] and elt["ServiceTier"]==tableElement["ServiceTier"] and elt["Sku"]==tableElement["Sku"]:
                    inList=True
                    eltIndex=i
                    #tableElement=elt
                    break
        
            if not inList:
                dates = sorted(select_output(lignesFacturation,"Date",True),key=lambda x:x,reverse=True)
                 
                    
                table.append(tableElement)
            else:
                table[eltIndex]["Nombre"]+=1

        else:
            print ("Problème : ",appService["Location"],appService["SkuSize"],appServicePrice,len(listeAppServicePrice))
            
            #print("Paramètres : ",appService["Location"],service,"",appService["SkuSize"],os, "CHF")
            break
    
        
        
    #Ligne total
    tableElement={
            "Nombre":sum(select_output(table,"Nombre",False)),
            "Location":"",
            "ServiceTier":"",
            "Sku":"",
            "On Demand":"",
            "1Y Reservation":"",
            "3Y Reservation":""
    }
    # table.append(tableElement)
    return (table)

#appServicePrices=get_azure_prices(service=service, currency="CHF")


def get_jointure_plan_facturation(lignesFacturation,listeAppService,service):
    dates=sorted(select_output(lignesFacturation,"Date",True),reverse=True)
    facturationService=[]
    for d in range(1,1):
        date=dates[d]
        facturationService+= list(filter(lambda x : not "fct-" in x["ResourceId"],list_filter(lignesFacturation,[("Date",date),("Application","Core Model"),("Service",service)])))
        counter=0
        for i in range(len(facturationService)):
            ligneFacturation=facturationService[i]
            appPlanElt=None
            appPlanElt = getRessourceInfos(listeAppService,ligneFacturation["ResourceId"])
            if appPlanElt==None:
                appPlanList=list_filter(listeAppService,[("ServerFarmId",ligneFacturation["ResourceId"])])
                if len(appPlanList)>0:
                    appPlanElt=appPlanList[0]
                
            if appPlanElt==None:
                counter+=1
                print(counter,"Décommissionné : ",ligneFacturation["ResourceId"])
                facturationService[i]["SkuTier"]=None
                facturationService[i]["SkuSize"]=None
                facturationService[i]["OS"]=None
                facturationService[i]["LocationPlan"]=None
            else:
                facturationService[i]["SkuTier"]=appPlanElt["SkuTier"]
                facturationService[i]["SkuSize"]=appPlanElt["SkuSize"]
                facturationService[i]["OS"]=appPlanElt["OS"]
                facturationService[i]["LocationPlan"]=appPlanElt["Location"]
    return(facturationService)



def get_price_table_and_usage(lignesFacturation,listeAppService,service):
    jointureFacturationService=get_jointure_plan_facturation(lignesFacturation,listeAppService,service)
    priceTable=get_price_table(service,list(filter(lambda app: app["SkuTier"] !="Dynamic" and app["SkuTier"]!="WorkflowStandard" and app["SkuTier"] !="ElasticPremium",  listeAppService)),appServicePrices)
     
    dates=sorted(select_output(lignesFacturation,"Date",True),reverse=True)
    for d in range(1,1):
        date=dates[d]
        for i in range(len(priceTable)):
            price=priceTable[i]
            
            
            if sum_outputs(list(filter(lambda x : x["OS"]==price["OS"] and x["PricingModel"]=="OnDemand" and x["LocationPlan"]==price["Location"] and x["SkuSize"]==price["Sku"],jointureFacturationService)),["Usage"])=={}:
                priceTable[i]["Usage OnDemand "+date]=0
            else:
                priceTable[i]["Usage OnDemand "+date]=sum_outputs(list(filter(lambda x : x["OS"]==price["OS"] and x["PricingModel"]=="OnDemand" and x["LocationPlan"]==price["Location"] and x["SkuSize"]==price["Sku"],jointureFacturationService)),["Usage"])[date]["Usage"]
        
        
            if sum_outputs(list(filter(lambda x : x["OS"]==price["OS"] and x["PricingModel"]=="Reservation" and x["LocationPlan"]==price["Location"] and x["SkuSize"]==price["Sku"],jointureFacturationService)),["Usage"])=={}:
                priceTable[i]["Usage Reservation "+date]=0
            else:
                priceTable[i]["Usage Reservation "+date]=sum_outputs(list(filter(lambda x : x["OS"]==price["OS"] and x["PricingModel"]=="Reservation" and x["LocationPlan"]==price["Location"] and x["SkuSize"]==price["Sku"],jointureFacturationService)),["Usage"])[date]["Usage"]
        
            
            if sum_outputs(list(filter(lambda x : x["OS"]==price["OS"] and x["LocationPlan"]==price["Location"] and x["SkuSize"]==price["Sku"],jointureFacturationService)),["Coût"])=={}:
                priceTable[i]["Coût "+date]=0
            else:
                priceTable[i]["Coût "+date]=sum_outputs(list(filter(lambda x : x["OS"]==price["OS"] and x["LocationPlan"]==price["Location"] and x["SkuSize"]==price["Sku"],jointureFacturationService)),["Coût"])[date]["Coût"]
        
            
            #priceTable[i]["Coût "+date]=list(filter(lambda x : x["OS"]==price["OS"] and x["PricingModel"]=="On Demand" and x["LocationPlan"]==price["Location"] and x["SkuSize"]==price["Sku"],jointureFacturationService))

            priceTable[i]["Coût horaire "+date]= get_price_rate(list(filter(lambda x : x["OS"]==price["OS"] and x["LocationPlan"]==price["Location"] and x["SkuSize"]==price["Sku"],jointureFacturationService)))
            if not priceTable[i]["Coût horaire "+date]==None and not priceTable[i]["On Demand"]==None and priceTable[i]["Coût horaire "+date]> priceTable[i]["On Demand"]:
                print(priceTable[i])
            
            priceTable[i]["Gains Réservation "+date]=None
            if( priceTable[i]["Gains Réservation "+date]!=None):   
                priceTable[i]["Gains Réservation "+date]=(priceTable[i]["On Demand"]-priceTable[i]["Coût horaire "+date])*(priceTable[i]["Usage OnDemand "+date]+priceTable[i]["Usage Reservation "+date])
            else:
                priceTable[i]["Gains Réservation "+date]=0
    return priceTable

# service="Azure App Service"
#appServicePrices=get_azure_prices(service)
#%store appServicePrices
#list_to_dataframe(get_azure_prices(service))
    
    
#azurePrice=get_azure_prices_region("Australia East",service,skuName, currency="CHF")

#table=get_price_table("Azure App Service",list(filter(lambda app: app["SkuTier"] !="Dynamic" and app["SkuTier"]!="WorkflowStandard" and app["SkuTier"] !="ElasticPremium",  listeAppService)),appServicePrices)
#list_to_dataframe(table)

# appServicePricesAndUsages=get_price_table_and_usage(lignesFacturation,listeAppService,"Azure App Service")
# list_to_dataframe(appServicePricesAndUsages)
# %store appServicePricesAndUsages



#vmPrices=get_azure_prices("Virtual Machines")

# %%
def plot_histogramme_double_combine(serie1,serie2,serie3,output,titre):
    dict1 = serie1[0]
    dict2 = serie2[0]
    dict3 = serie3[0]

    # Définition de la police de caractères
    font = font_manager.FontProperties(family='Arial', size=10)

    # Création du graphique
    fig, ax = plt.subplots(figsize=(10, 6))
    ax2 = ax.twinx()
    
    
    # Données
    dates = sorted(set(dict1.keys()) | set(dict2.keys()) | set(dict3.keys()))
    labels = [datetime.strptime(l, '%Y-%m-%d').strftime('%b %y') for l in dates]
    plt.xticks(rotation=90, ha='center', fontproperties=font,fontsize=6)
    
    data1 = [dict1.get(d, {}).get(output, 0) for d in dates]
    data2 = [dict2.get(d, {}).get(output, 0) for d in dates]
    data3 = [dict3.get(d, {}) for d in dates]

    # Création des barres empilées sur le premier axe
    bars1 = ax.bar(labels, data1, label=serie1[1], color="gray")
    bars2 = ax.bar(labels, data2, bottom=data1, label=serie2[1], color="#0089D6")

    # Création de la courbe sur le deuxième axe
    ax2.plot(labels, data3, color='red', label=serie3[1])

   
     # Ajout des étiquettes avec les valeurs
    for bar in bars1:
        height = bar.get_height()
        if int(height)>0 :
            valeur = int(height)
            ax.text(bar.get_x() + bar.get_width()/2., bar.get_y()+bar.get_height()/2., '{:,.0f}'.format(valeur).replace(',', "'"), ha='center', va='center', fontproperties=font,fontsize=9,rotation=90)
        
        else:
            valeur = 0    
            #ax.text(bar.get_x() + bar.get_width()/2., bar.get_y()+bar.get_height()/2., "", ha='center', va='center', fontproperties=font,fontsize=9,rotation=90)
           
        
    for bar in bars2:
        height = bar.get_height()
        if int(height)>0 :
            valeur = int(height)
            ax.text(bar.get_x() + bar.get_width()/2., bar.get_y()+bar.get_height()/2., '{:,.0f}'.format(valeur).replace(',', "'"), ha='center', va='center', fontproperties=font,fontsize=9,rotation=90)
        
        else:
            valeur = 0
    
    

    if "Coût" in output:
        ax.set_ylabel(output + " (CHF)", fontproperties=font, fontsize=10)
    else:
        ax.set_ylabel(output, fontproperties=font, fontsize=10)

    ax2.set_ylabel(serie3[1], fontproperties=font, fontsize=10)
    ax2.yaxis.set_major_formatter(mticker.PercentFormatter(1.0, decimals=0))

            
    


    # Configuration de l'axe des y
    #Y max
    y_max=0
    for i in range(len(bars1)):
        height1=bars1[i].get_height()
        height2=bars2[i].get_height()
        y_max=max(y_max,(int(height1)+int(height2))*1.01)
     
    y_max= math.ceil(y_max/1000)*1000

    
    ax2.set_ylim(0, 1)
    ax.set_ylim(0, y_max)
    
    # if "Coût" in output:
    #     plt.ylabel(output+ " (CHF)", fontproperties=font,fontsize=10)
    # else:
    #     plt.ylabel(output, fontproperties=font,fontsize=10)

    # Configuration du titre
    plt.title(titre, fontproperties=font)
    
    # Configuration de l'axe des x
    
    # Affichage du graphique
    ax.legend(loc="upper left",fontsize=9)
    plt.legend(loc="upper right",fontsize=9)
    plt.show()  
    
    


# %%
#ANALYSE APP SERVICE V3

# coutAppServiceOnDemand = sum_outputs(list_filter(lignesFacturation, filtres+[("PricingModel","OnDemand")]),["Usage","Usage journalier"])
# coutAppServiceReservation  = sum_outputs(list_filter(lignesFacturation, filtres+[("PricingModel","Reservation")]),["Usage","Usage journalier"])
# plot_histogramme_double_combine((coutAppServiceOnDemand,"On Demand"),(coutAppServiceReservation,"Reservation"),(get_reservation_rate(lignesFacturation,"Azure App Service","Azure App Service Premium v3 Plan",True),"Taux réservation App Service v3"),"Usage journalier","Historique usage journalier moyen Azure App Service Premium v3 Plan (Core Model)")


# %%
# data=lignesFacturation
# #data=tes

# filtres = [("Service","Azure App Service"),("ServiceTier","Azure App Service Premium v3 Plan")]

# usageAppService = sum_outputs(list_filter(data, filtres),["Usage","Usage journalier","Coût","Coût journalier"])
# plot_histogramme((usageAppService,"Usage total journalier"),"Usage journalier","Historique usage journalier moyen Azure App Service Premium v3 Plan (Core Model)")
# plot_histogramme((usageAppService,"Coût total journalier"),"Coût journalier","Historique coût journalier moyen Azure App Service Premium v3 Plan (Core Model)")

# usageAppServiceOnDemand = sum_outputs(list_filter(data, filtres+[("PricingModel","OnDemand")]),["Usage","Usage journalier","Coût","Coût journalier"])
# usageAppServiceReserve = sum_outputs(list_filter(data, filtres+[("PricingModel","Reservation")]),["Usage","Usage journalier"])
# plot_histogramme_double((usageAppServiceOnDemand,"On Demand"),(usageAppServiceReserve,"Reservation"),"Usage journalier","Historique usage journalier moyen Azure App Service Premium v3 Plan (Core Model)")

# coutAppServiceOnDemand = sum_outputs(list_filter(data, filtres+[("PricingModel","OnDemand")]),["Coût","Coût journalier"])
# coutAppServiceReservation  = sum_outputs(list_filter(data, filtres+[("PricingModel","Reservation")]),["Coût","Coût journalier"])
# plot_histogramme_double((coutAppServiceOnDemand,"On Demand"),(coutAppServiceReservation,"Reservation"),"Coût journalier","Historique coût journalier moyen Azure App Service Premium v3 Plan (Core Model)")

# %%
def get_instances_decommissionees(lignesFacturation):
    dates=sorted(select_output(lignesFacturation,"Date",True),reverse=False)
    instancesDecom=[]
    
    for i in range(len(dates)-1):
    #i=0
        instancesM=select_outputs(list_filter(lignesFacturation,[("Date",dates[i])]),["ResourceId","Coût"])
        instancesM1=select_outputs(list_filter(lignesFacturation,[("Date",dates[i+1])]),["ResourceId","Coût"])
        for instance in instancesM:   
            inList=False
            for instance1 in instancesM1:
                if instance["ResourceId"]==instance1["ResourceId"]:
                    inList=True
                    break
            if not inList:
                instancesDecom.append({
                    "Date":dates[i],
                    "ResourceId":instance["ResourceId"],
                    "Coût":instance["Coût"]
                })
    return instancesDecom




# %%
def gainsObtenus(data):
    decommissionnement=sum_outputs(get_instances_decommissionees(data),["Coût"])
    usages=sum_outputs(data,["Usage","Usage journalier"])
    tarifsReels=get_price_rates(data)
    gainsReservation=sum_outputs(list_filter(appServicePricesAndUsages,[("ServiceTier","Azure App Service Premium v3 Plan")]),"Gains Réservation
    return tarifsOnDemand

filtres = [("Application","Core Model"),("Service","Azure App Service"),("ServiceTier","Azure App Service Premium v3 Plan")]


  
# gainsObtenus(list_filter(lignesFacturation,filtres))

# %%
def doQueryCost(scope, body):
    url = "https://management.azure.com/"+scope+"/providers/Microsoft.CostManagement/query?api-version=2021-10-01"
    response = doRequest("POST",url, body)  
    return response

end_date = datetime.now()
start_date = end_date - timedelta(days=365)
    # Récupération des coûts du mois en cours pour le Management Group spécifié
    #url = "{}/{}".format(base_url, management_group_id)
    
    
token = getToken()    
scope="providers/Microsoft.Management/managementGroups/mg-rolex"
body={"type": "AmortizedCost",
        "timeframe": "Custom",
        "timePeriod": {
            "from": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        },
        "Dataset": {
            "granularity": "Monthly",
            "aggregation": {
                "totalCost": {
                    "name": "Cost",
                    "function": "Sum"
                },
                "UsageQuantity": {
                    "name": "UsageQuantity",
                    "function": "Sum",
                    "unit": "Hours"
                }
            },
        "grouping": [
            {
                "type": "Dimension",
                "name": "ServiceName"
            },
            {
                "type":"Dimension",
                "name":"ChargeType",
                },
            {
                "type":"Dimension",
                "name":"Location",
                }
    ]},
        
        "filter": 
                   
                        
                            {
                            "dimensions": {
                                "name": "ChargeType",
                                "operator": "In",
                                "values": ["UnusedReservation"]
                            }
                        }
                            
                    
                
            
        
    }
# token=getToken()
# response = doQueryCost(scope, body)
# list_to_dataframe(response.json()["properties"]["rows"])



# %%
def get_lignes_usage(management_group_id):
      
    startTime=time.time()
    
    subscriptions=getSubscriptions()
    
    
     # Définition des paramètres de l'API Cost Management
    url = "https://management.azure.com/providers/Microsoft.Management/managementGroups/"+management_group_id+"/providers/Microsoft.CostManagement/query?api-version=2021-10-01"
    #api_version = "2019-11-01-preview"

    # Récupération du token d'accès Azure
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    # Récupération des coûts du mois en cours pour le Management Group spécifié
    #url = "{}/{}".format(base_url, management_group_id)
    body = {
        "type": "Usage",
        "timeframe": "Custom",
        "timePeriod": {
            "from": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            # "from":"2023-03-01T00:00:00Z",
            # "to":"2023-04-01T00:00:00Z"
        },
        "Dataset": {
            "granularity": "Monthly",
            "aggregation": {
                "UsageQuantity": {
                    "name": "UsageQuantity",
                    "function": "Sum"
                }
            },
            "grouping": [
                {
                    "type": "Dimension",
                    "name": "ResourceGroupName"
                },
                {
                    "type": "Dimension",
                    "name": "ServiceName"
                },
                {
                    "type": "Dimension",
                    "name": "MeterId"
                },
                {
                    "type": "Dimension",
                    "name": "SubscriptionId"
                },
                {
                    "type": "Dimension",
                    "name": "PricingModel"
                },
                {
                    "type": "Dimension",
                    "name": "ReservationName"
                }
            ]
        
        },
        "options": {
            "includeReservedInstances": True
        }
    }
    
    costs = []
    counter = 1
    
    while len(url)>0:
        print(str(counter) + " : " + url)
        response = doRequest("POST",url, body)
        
        if not response is None :
            data = response.json()
            application={}
            
            for item in data["properties"]["rows"]:
                application=getApplication(subscriptions,item)
                cost = {
                    "Date": item[1].replace("T00:00:00",""),
                    "Subscription": item[5],
                    "Abonnement":application["Abonnement"],
                    "Application":application["Application"],
                    "ResourceGroup": item[2],
                    "Service": item[3],
                    "MeterId":item[4],                    
                    "PricingModel":item[6],
                    "Reservation":item[7],
                    "Usage": item[0],
                    "Usage journalier":item[0]/max(nombre_jours_mois(item[1].replace("T00:00:00","")),1)              
                }
                costs.append(cost)
            
            url=data["properties"]["nextLink"]
            #url=""
            counter+=1
        else:
            print(f"Echec - Historique lignes usages : Requête incomplète") 
            return costs
    
    print("Succès - Historique lignes usages : " + str(int((time.time()-startTime)/60)) + " minutes")
    return costs  

# %%
def calculate_reservation_rate(subscription_id, resource_group_name):
    # Calculate the start and end date of the 6 months period
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=180)

    # Define the API endpoint to get the reservations
    endpoint = f"https://management.azure.com/providers/Microsoft.Billing/billingAccounts/5957027/providers/Microsoft.Consumption/reservationSummaries?api-version=2023-03-01&grain=Monthly"

    # Add the Bearer token to the headers of the request
    headers = {
        "Authorization": f"Bearer {token}"
    }

    # Make the request to the API using the requests module
    response=REQ.get(endpoint,headers=headers)
    #response = doRequest("GET",endpoint, "")

    # Check the status code of the response
    print(response)
    if response.status_code == 200:
        # Parse the response as JSON
        data = response.json()

        # Calculate the total and reserved instances from the response
        total_instances = sum([summary["instanceFlexibility"] + summary["instanceSizeFlexibility"] for summary in data["value"]])
        
    return total_instances

# print(calculate_reservation_rate("8d5552b1-0631-4da9-ac32-931980a54230","rg-hk-cmss-prod"))

# %%
#TEST RECOMMENDATIONS

def testReco(subscriptionId):
    print("Get recommendations : "+ subscriptionId)
    url=f"https://management.azure.com{subscriptionId}/providers/Microsoft.CostManagement/benefitRecommendations?api-version=2022-10-01"
    recos=[]
    while len(url)>0:
        response=doRequest("GET",url,"")
        data=response.json()
        recos+=data["value"]
        if "nextLink" in data.keys() and data["nextLink"]!=None:
            url=data["nextLink"]
        else:
            url=""
    return(data["value"])



def testGetRecos():
    subscriptions=getSubscriptions()
    recos=[]
    
    for sub in subscriptions:
        recosSub=(testReco(sub["id"]))
        for r in recosSub:
            recos.append(r)
    return recos
        
# testGetRecos()

# %% [markdown]
# EXPORT ASLAM

# %%
def get_lignes_facturation_mois_excel(management_group_id,d,skipToken=""):
    
    startTime=time.time()
     # Définition des paramètres de l'API Cost Management
    url = "https://management.azure.com/providers/Microsoft.Management/managementGroups/"+management_group_id+"/providers/Microsoft.CostManagement/query?api-version=2021-10-01"
    if not skipToken=="":
        url+=("&$skiptoken="+skipToken)
        
    #api_version = "2019-11-01-preview"

    # Récupération du token d'accès Azure
    start_date,end_date = get_current_month(d,True)
    # end_date = datetime.now()
    # start_date = end_date - timedelta(days=365)
    # Récupération des coûts du mois en cours pour le Management Group spécifié
    #url = "{}/{}".format(base_url, management_group_id)
    body = {
        "type": "AmortizedCost",
        "timeframe": "Custom",
        "timePeriod": {
            "from": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        },
        "Dataset": {
            "granularity": "Monthly",
            "aggregation":{"totalCost":{"name":"Cost","function":"Sum"}},
            "grouping":[{"type":"Dimension","name":"SubscriptionId"},
                {"type":"Dimension","name":"ServiceName"},
                {"type":"Dimension","name":"ResourceGroupName"},
                {"type":"Dimension","name":"ResourceLocation"}]
	    }
    }
    
    costs = []
    counter = 0
    
    while len(url)>0:
        
        response = doRequest("POST",url, body)

        skipToken=""
        
        if response is None:
            token=getToken()
            #print("Sleep",response)
            time.sleep(80)
            response = doRequest("POST",url, body)
        
        if response is None:
            # token=getToken()
            #print("Sleep",response)
            time.sleep(300)
            response = doRequest("POST",url, body)
        
        if not response is None :
            
            data = response.json()
            print(str(counter)+" : "+url)  
            application={}
            
            for item in data["properties"]["rows"]:
           
                application=get_application(subscriptions,item[2],item[4])
                location=get_infos_location(subscriptions,item[2],item[5])
                # if location==None:
                    # print("Check location",item[2],item[5])
                # print(location)
                # return
            
                cost = {
                    "Date": item[1].replace("T00:00:00",""),
                    "SubscriptionId": item[2],
                    "SubscriptionName":application["Abonnement"],
                    "Application":application["Application"],
                    "Environnement":application["Environnement"],
                    "ResourceGroup":item[4],
                    "LocationName":location["name"],
                    "LocationLatitude":location["latitude"],
                    "LocationLongitude":location["longitude"],
                    "LocationId":location["id"],
                    "LocationCountry":location["country"],
                    "LocationCategory":location["regionCategory"],
                    "Service": item[3],
                    "Cout":item[0],
                    "Currency":item[6]
                }
                
                costs.append(cost)
                
            return (costs,"")
            url=data["properties"]["nextLink"]
            # print(url)
            if url=="" or url==None:
                skipToken=""
                url=""
            else:
                skipToken=url.split("$skiptoken=")[1]
            counter+=1
            # return (costs,"")
        else:
            print(f"Echec - Historique lignes facturation : Requête incomplète") 
            return (costs,skipToken)
    
    print("Succès - Historique lignes facturation : " + str(int((time.time()-startTime)/60)) + " minutes")
    return (costs,skipToken) 

def get_lignes_facturations_excel(mgGroup,start_date,end_date,path):
    listeMois=generate_month_list(start_date,end_date)
    lignesFacturation=[]
    
    for mois in listeMois:
        costs,skipToken = get_lignes_facturation_mois_excel(mgGroup,mois,"")

        if skipToken=="":
            df = pd.DataFrame(costs)

            # previous_df = pd.read_excel(path, sheet_name='1 - Historique coûts')
            # Create an ExcelWriter object and specify the output file
            writer = pd.ExcelWriter(path, engine='xlsxwriter')

            # new_df = pd.concat([previous_df, df], ignore_index=True)
            # Write the DataFrame to a specific sheet in the Excel file
            df.to_excel(writer, sheet_name='1 - Historique coûts', index=False)

            # Save the changes and close the ExcelWriter object
            writer.save()
            writer.close()
            print("Requête complète ("+mois.strftime("%Y-%m-%d")+")")
        else:
            print("Requête incomplète ("+mois.strftime("%Y-%m-%d")+") : ",skipToken)
       # break
    print("Requête terminée")
    return lignesFacturation
    
#test=get_lignes_facturations("2023-01-01")
token=getToken()
start_date="2023-06-01"
end_date="2023-06-01"
path="C:\\Users\\hugo.dufrene\\OneDrive - Wavestone\\Documents\\FinOps\\Variables\\Export Coûts Azure - Tableau v3.xlsx"
lignesFacturation=get_lignes_facturations_excel(mgGroup,start_date,end_date,path)

# %%
def get_lignes_VM_mois_excel(management_group_id,d,skipToken=""):
    
    startTime=time.time()
     # Définition des paramètres de l'API Cost Management
    url = "https://management.azure.com/providers/Microsoft.Management/managementGroups/"+management_group_id+"/providers/Microsoft.CostManagement/query?api-version=2021-10-01"
    if not skipToken=="":
        url+=("&$skiptoken="+skipToken)
        
    #api_version = "2019-11-01-preview"

    # Récupération du token d'accès Azure
    start_date,end_date = get_current_month(d,True)
    # end_date = datetime.now()
    # start_date = end_date - timedelta(days=365)
    # Récupération des coûts du mois en cours pour le Management Group spécifié
    #url = "{}/{}".format(base_url, management_group_id)
    body = {
        "type": "AmortizedCost",
        "timeframe": "Custom",
        "timePeriod": {
            "from": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        },
        "Dataset": {
            "granularity": "Monthly",
            "aggregation": {
                "totalCost": {
                    "name": "Cost",
                    "function": "Sum"
                },
                "UsageQuantity": {
                    "name": "UsageQuantity",
                    "function": "Sum",
                    "unit": "Hours"
                }
            },
            
            "grouping": [{"type":"Dimension","name":"SubscriptionId"},
	           {"type": "Dimension","name": "ServiceName"},
                {"type":"Dimension","name":"ResourceGroupName"},
	           {"type": "Dimension","name": "PricingModel"},
                {"type": "Dimension","name": "Meter"},
                {"type": "Dimension","name": "ResourceId"}],
            "filter":{"dimensions": {
                        "name": "ServiceName","operator": "In","values": ["Virtual Machines"]
                        }}

                }
    }
    
    costs = []
    counter = 1
    
    while len(url)>0:
        
        response = doRequest("POST",url, body)

        skipToken=""
        
        if response is None:
            token=getToken()
            print("Response None 1")
            #print("Sleep",response)
            time.sleep(80)
            response = doRequest("POST",url, body)
        
        if response is None:
            # token=getToken()
            print("Response None 2")
            #print("Sleep",response)
            time.sleep(300)
            response = doRequest("POST",url, body)
        
        if not response is None :
            
            data = response.json()
            print(str(counter)+" : "+url)  
            application={}
            
            for item in data["properties"]["rows"]:

                application=get_application(subscriptions,item[3],item[5])
                cost = {
                    "Date": item[2].replace("T00:00:00",""),
                    # "JoursMois":max(nombre_jours_mois(item[2].replace("T00:00:00","")),1),
                    "SubscriptionId": item[3],
                    "SubscriptionName":application["Abonnement"],
                    "Application":application["Application"],
                    "Service": item[4],
                    "ResourceGroup": item[5],
                    "PricingModel":item[6],
                    "VmId":item[8],
                    "Sku":item[7],
                    "Usage":item[1],
                    "Cost":item[0],
                    "Currency":item[9]
                }
                costs.append(cost)
                

            
            url=data["properties"]["nextLink"]
            # print(url)
            if url=="" or url==None:
                skipToken=""
                url=""
            else:
                skipToken=url.split("$skiptoken=")[1]
            counter+=1
        else:
            print(f"Echec - Historique lignes facturation : Requête incomplète") 
            return (costs,skipToken)
    
    print("Succès - Historique lignes facturation : " + str(int((time.time()-startTime)/60)) + " minutes")
    return (costs,skipToken) 

def get_lignes_VM_excel(mgGroup,start_date,end_date,path):
    listeMois=generate_month_list(start_date,end_date)
    lignesFacturation=[]
    
    for mois in listeMois:
        costs,skipToken = get_lignes_VM_mois_excel(mgGroup,mois,"")

        if skipToken=="":
            df = pd.DataFrame(costs)

            previous_df = pd.read_excel(path, sheet_name='2 - Virtual Machines')
            # Create an ExcelWriter object and specify the output file
            writer = pd.ExcelWriter(path, engine='xlsxwriter')

            new_df = pd.concat([previous_df, df], ignore_index=True)
            # Write the DataFrame to a specific sheet in the Excel file
            new_df.to_excel(writer, sheet_name='2 - Virtual Machines', index=False,header=True)

            # Save the changes and close the ExcelWriter object
            writer.save()
            print("Requête complète ("+mois.strftime("%Y-%m-%d")+")")
        else:
            print("Requête incomplète ("+mois.strftime("%Y-%m-%d")+") : ",skipToken)
       # break
    return lignesFacturation
    
#test=get_lignes_facturations("2023-01-01")
# token=getToken()
start_date="2022-01-01"
end_date="2023-06-01"
path="C:\\Users\\hugo.dufrene\\OneDrive - Wavestone\\Documents\\FinOps\\Variables\\Export Coûts Azure - Aslam 2.xlsx"
# token=get_token()
# print("TOKEN")
get_lignes_VM_excel(mgGroup,start_date,end_date,path)

# %%
def format_lignes_VM_excel(path):
    df = pd.read_excel(path, sheet_name='2 - Virtual Machines')
    on_demand_df = df[df["PricingModel"] == "OnDemand"].rename(columns={"Usage": "UsageOnDemand", "Cost": "CostOnDemand"})
    reservation_df = df[df["PricingModel"] == "Reservation"].rename(columns={"Usage": "UsageReservation", "Cost": "CostReservation"})
    merged_df = pd.merge(df[["Date","SubscriptionId","SubscriptionName","Application","Service","ResourceGroup","VmId","Sku"]], on_demand_df[['Date', 'VmId', 'UsageOnDemand', 'CostOnDemand']], on=['Date', 'VmId'], how='left')
    merged_df = pd.merge(merged_df, reservation_df[['Date', 'VmId', 'UsageReservation', 'CostReservation']], on=['Date', 'VmId'], how='left')
    merged_df = merged_df.drop_duplicates(['Date',"SubscriptionId", 'VmId'])
    merged_df=merged_df.fillna(0)
    writer = pd.ExcelWriter(path, engine='xlsxwriter')

    merged_df.to_excel(writer, sheet_name='3 - Virtual Machines Formatted', index=False,header=True)

    writer.save()
    return merged_df

    
# df=format_lignes_VM_excel(path)
# df

# %%
start,end=get_current_month(datetime(2022, 1, 1, 0, 0),True)


# %%
def get_vm_CPU(path1,path2):
    df = pd.read_excel(path1, sheet_name='3 - Virtual Machines Formatted')
    print(len(df))
    df["CPU Moyen"]=df.apply(lambda row:0.0)

    df=df.sort_values("Date",ascending=False)
    counter=0
    for i in range(len(df)):
        row = df.iloc[i]
    
        start,end=get_current_month(datetime.strptime(str(row["Date"]), "%Y-%m-%d"),True)
        # print(start.strftime("%Y-%m-%dT%H:%M:%SZ"),end.strftime("%Y-%m-%dT%H:%M:%SZ"))
        
        # vmId="/subscriptions/14731824-aafc-4d86-b844-3d90f8eacd65/resourcegroups/rg-ph-infra-common/providers/microsoft.compute/virtualmachines/saz-ph-0016"
        url= "https://management.azure.com"+row["VmId"]+"/providers/Microsoft.Insights/metrics?api-version=2021-05-01&metricnames=Percentage%20CPU&interval=P1D&timespan="+start.strftime("%Y-%m-%dT%H:%M:%SZ")+"/"+end.strftime("%Y-%m-%dT%H:%M:%SZ")

        response = doRequest("GET",url,"")
        
        if response==None:
            print("Max retention :",row["Date"],"=>",row["VmId"])
            
            # if counter>10:
            #     break
            counter+=1
        else: 
            if len(response.json()["value"])>0:
                if "timeseries" in response.json()["value"][0].keys():
                    if len(response.json()["value"][0]["timeseries"])>0:
                        if "data" in response.json()["value"][0]["timeseries"][0].keys():
                            data = response.json()["value"][0]["timeseries"][0]["data"]
                            data_df=pd.DataFrame(data)
                            CPU_average=data_df.mean()
                            df["CPU Moyen"].iloc[i]=CPU_average
                            counter=0
            
        if i%10==0:
            print("Avancement","{:.2%}".format(i/len(df))," %","=>",row["Date"])
            
        if i%1000==0:
            writer = pd.ExcelWriter(path2, engine='xlsxwriter')
            
            # Write the DataFrame to a specific sheet in the Excel file
            df.to_excel(writer, sheet_name='4 - Virtual Machines CPU', index=False,header=True)

            writer.save()
            print("Saved CPU",i,"/",len(df),"=>","{:.2%}".format(i/len(df))," %")
    print("Fin collecte CPU")
    return df

    
token=getToken()
path2="C:\\Users\\hugo.dufrene\\OneDrive - Wavestone\\Documents\\FinOps\\Variables\\Export Coûts Azure - Aslam 3.xlsx"

df_CPU=get_vm_CPU(path,path2)
    

# %%
def get_cost_usage(subscription_id):
    credential =ClientSecretCredential(client_id="258d0525-a80c-4b29-bf60-9bd65ac5d418", client_secret="p3Q8Q~VOkEdF-hC-wfuNf.aUrwycYNiNpAibAakZ", tenant_id="f2460eca-756e-4a3f-bd14-d2a84590fc31")
    # client = CostManagementClient(credential, subscription_id)

    lignesFacturation=[]
    
    cost_client = CostManagementClient(credential)

    # Create a QueryDefinition object
    query = QueryDefinition(
        type="AmortizedCost",
        timeframe="Custom",
        time_period={
            "from": datetime(2023, 1, 1),
            "to": datetime(2023, 1, 31)
        },
        dataset=QueryDataset(
            granularity="Monthly",
            aggregation={"totalCost":{"name":"Cost","function":"Sum"}},
            grouping=[{"type":"Dimension","name":"SubscriptionId"},
                {"type":"Dimension","name":"ServiceName"},
                {"type":"Dimension","name":"ResourceGroupName"},
                {"type":"Dimension","name":"ResourceLocation"}]
        )
        
    )

    # Query the cost data
    response = cost_client.query.usage(scope="/providers/Microsoft.Management/managementGroups/mg-rolex",parameters=query)

    # Process the response
    for row in response.rows:
        cost, date,subscription_id,service,resourceGroup,location,currency=row
        application=get_application(subscriptions,subscription_id,resourceGroup)
        
        lignesFacturation.append(
            {"Date":date,
             "AbonnementId":subscription_id,
             "Abonnement":application["Abonnement"],
             "Application":application["Application"],
             "Environnement":application["Environnement"],
             "Service":service,
             "ResourceGroup":resourceGroup,
             "Region":location,
             "Coût":cost,
             "Devise":currency
            }
        )
        
    return lignesFacturation
        
print(get_cost_usage("00fd21d5-9fda-4aaa-88f5-1b88f385006c"))


