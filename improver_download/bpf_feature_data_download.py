# 2023 (C) Crown Copyright, Met Office. All rights reserved.
#
# This file is part of Weather DataHub and is released under the
# BSD 3-Clause license.
# See LICENSE in the root of the repository for full licensing details.
# (c) Met Office 2023

import requests
import argparse
import time
import sys
import logging as log

log.basicConfig(filename='bpf_feature_data_download.log',
                filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')

base_url = "https://data.hub.api.metoffice.gov.uk/mo-site-specific-blended-probabilistic-forecast/1.0.0/"
allowed_collection_ids = ["improver-percentiles-spot-global",
                          "improver-probabilities-spot-global",
                          "improver-probabilities-spot-uk",
                          "improver-percentiles-spot-uk"]


def retrieve_forecast(base_url, collection_id, request_headers, location_id):
    url = base_url + "collections/" + collection_id + "/locations/" + location_id

    headers = {'accept': "application/json"}
    headers.update(request_headers)

    success = False
    retries = 5

    while not success and retries > 0:
        try:
            req = requests.get(url, headers=headers)
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
        "-c",
        "--collection",
        action="store",
        dest="collection_id",
        default="",
        help="Provide a collection_id found in the /collections endpoint. The options are "
             "improver-percentiles-spot-global, "
             "improver-probabilities-spot-global, "
             "improver-probabilities-spot-uk, "
             "improver-percentiles-spot-uk",
    )
    parser.add_argument(
        "-l",
        "--location",
        action="store",
        dest="location_id",
        default="",
        help="Provide a location_id found in the /{collection_id}/locations endpoint."
    )
    parser.add_argument(
        "-k",
        "--apikey",
        action="store",
        dest="apikey",
        default="",
        help="REQUIRED: Your WDH API Credentials."
    )

    args = parser.parse_args()

    collection_id = args.collection_id
    location_id = args.location_id
    apikey = args.apikey

    # Client API key must be supplied
    if apikey == "":
        print("ERROR: API credentials must be supplied.")
        sys.exit()
    else:
        requestHeaders = {"apikey": apikey}

    if collection_id not in allowed_collection_ids:
        print("ERROR: The available collection IDs are: ")
        print(*allowed_collection_ids, sep="\n")
        sys.exit()

    retrieve_forecast(base_url, collection_id, requestHeaders, location_id)
