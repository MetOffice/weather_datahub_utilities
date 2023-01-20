import requests
import argparse
import time
import sys
import logging as log

log.basicConfig(filename='ss_download.log', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')

base_url = "https://api-metoffice.apiconnect.ibmcloud.com/v0/forecasts/point/"

def retrieve_forecast(baseUrl, timesteps, requestHeaders, latitude, longitude, excludeMetadata, includeLocation):
    
    url = baseUrl + timesteps 
    
    headers = {'accept': "application/json"}
    headers.update(requestHeaders)
    params = {
        'excludeParameterMetadata' : excludeMetadata,
        'includeLocationName' : includeLocation,
        'latitude' : latitude,
        'longitude' : longitude
        }

    success = False
    retries = 5

    while not success and retries >0:
        try:
            req = requests.get(url, headers=headers, params=params)
            success = True
        except Exception as e:
            log.warning("Exception occurred", exc_info=True)
            retries -= 1
            time.sleep(10)
            if retries == 0:
                log.error("Retries exceeded", exc_info=True)
                sys.exit()

    req.encoding = 'utf-8'

    print(req.text)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Retrieve the site-specific forecast for a single location"
    )
    parser.add_argument(
        "-t",
        "--timesteps",
        action="store",
        dest="timesteps",
        default="hourly",
        help="The frequency of the timesteps provided in the forecast. The options are hourly, three-hourly or daily",
    )
    parser.add_argument(
        "-c",
        "--client",
        action="store",
        dest="clientId",
        default="",
        help="REQUIRED: Client ID of your WDH Site-specific subscription",
    )
    parser.add_argument(
        "-s",
        "--secret",
        action="store",
        dest="secret",
        default="",
        help="REQUIRED: Your WDH API Site-specific client secret",
    )
    parser.add_argument(
        "-m",
        "--metadata",
        action="store",
        dest="excludeMetadata",
        default="FALSE",
        help="Provide a boolean value for whether parameter metadata should be excluded."
    )
    parser.add_argument(
        "-n",
        "--name",
        action="store",
        dest="includeLocation",
        default="TRUE",
        help="Provide a boolean value for whether the location name should be included."
    )
    parser.add_argument(
        "-y",
        "--latitude",
        action="store",
        dest="latitude",
        default="",
        help="Provide the latitude of the location you wish to retrieve the forecast for."
    )
    parser.add_argument(
        "-x",
        "--longitude",
        action="store",
        dest="longitude",
        default="",
        help="Provide the longitude of the location you wish to retrieve the forecast for."
    )

    args = parser.parse_args()

    clientId = args.clientId
    secret = args.secret
    timesteps = args.timesteps
    includeLocation = args.includeLocation
    excludeMetadata = args.excludeMetadata
    latitude = args.latitude
    longitude = args.longitude

    # Client ID and Sectet must be supplied
    if clientId == "" or secret == "":
        print("ERROR: IBM clientId and secret must be supplied.")
        sys.exit()
    else:
        requestHeaders = {"x-ibm-client-id": clientId,
                          "x-ibm-client-secret": secret}
    if latitude == "" or longitude == "":
        print("ERROR: Latitude and longitude must be supplied")
        sys.exit()

    if timesteps != "hourly" and timesteps != "three-hourly" and timesteps != "daily":
        print("ERROR: The available frequencies for timesteps are hourly, three-hourly or daily.")
        sys.exit() 
    
    retrieve_forecast(base_url, timesteps, requestHeaders, latitude, longitude, excludeMetadata, includeLocation)




