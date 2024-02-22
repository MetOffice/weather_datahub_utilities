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

log.basicConfig(filename='bpf_feature_collections.log', filemode='w',
                format='%(asctime)s - %(levelname)s - %(message)s')

base_url = "https://data.hub.api.metoffice.gov.uk/mo-site-specific-blended-probabilistic-forecast/1.0.0/collections"


def retrieve_collections(base_url, request_headers):
    headers = {'accept': "application/json"}
    headers.update(request_headers)

    success = False
    retries = 5

    while not success and retries > 0:
        try:
            req = requests.get(base_url, headers=headers)
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
        "-k",
        "--apikey",
        action="store",
        dest="apikey",
        default="",
        help="REQUIRED: Your WDH API Credentials."
    )

    args = parser.parse_args()
    apikey = args.apikey

    # Client API key must be supplied
    if apikey == "":
        print("ERROR: API credentials must be supplied.")
        sys.exit()
    else:
        requestHeaders = {"apikey": apikey}

    retrieve_collections(base_url, requestHeaders)
