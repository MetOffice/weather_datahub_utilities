# 2022 (C) Crown Copyright, Met Office. All rights reserved.
#
# This file is part of Weather DataHub and is released under the
# BSD 3-Clause license.
# See LICENSE in the root of the repository for full licensing details.
# (c) Met Office 2022

import csv
import os
import sys
import requests
import argparse
import time
from datetime import datetime, timedelta
import queue
import threading
import uuid
import pprint

# Example code to download GRIB data files from the Met Office Weather DataHub via API calls

MODEL_LIST = ["mo-global", "mo-uk", "mo-uk-latlon", "mo-mogrepsg"]
BASE_URL = "https://api-metoffice.apiconnect.ibmcloud.com/metoffice/production/1.0.0"
debugMode = False
printUrl = False
retryCount = 3


def get_order_details(
    baseUrl, requestHeaders, orderName, useEnhancedApi, runsToDownload
):

    details = None

    actualHeaders = {"Accept": "application/json"}
    actualHeaders.update(requestHeaders)

    url = baseUrl + "/orders/" + orderName + "/latest"
    if useEnhancedApi:
        url = url + "?detail=MINIMAL"
        if len(runsToDownload) == 1:
            url = url + "&runfilter=" + runsToDownload[0]

    req = requests.get(url, headers=actualHeaders)

    if verbose and apikey == "":
        print("Plan and limit : " + req.headers["X-RateLimit-Limit"])
        print("Remaining calls: " + req.headers["X-RateLimit-Remaining"])

    if req.url.find("--") != -1:
        if verbose:
            print("-- found in redirect: ", req.url)

    if printUrl == True:
        print("get_order_details: ", url)
        if url != req.url:
            print("redirected to: ", req.url)

    if req.status_code != 200:
        print(
            "ERROR: Unable to load details for order : ",
            orderName,
            " status code: ",
            req.status_code,
        )
        print("Headers: ",req.headers)
        print("Text: ",req.text)
        print("URL:", url)
        sys.exit(6)
    else:
        details = req.json()

    return details


def get_order_file(
    baseUrl, requestHeaders, orderName, fileId, guidFileNames, folder, start, backdatedDate
):

    # If file id is too long or random file names required generate a uuid for the file name

    urlMod = ""
    global debugMode

    if len(fileId) > 100 or guidFileNames:
        local_filename = folder + "/" + str(uuid.uuid4()) + ".grib2"
    else:
        local_filename = folder + "/" + fileId + ".grib2"

    ttfb = 0

    if backdatedDate != "":
        if debugMode == True:
            print("DEBUG: We are in backdated Date mode for the date: " + backdatedDate)
        fileId = fileId.replace("+", backdatedDate)
        if debugMode == True:
            print("DEBUG: New fileID is: " + fileId)
        


        
    url = baseUrl + "/orders/" + orderName + "/latest/" + fileId + "/data"

    if debugMode == True:
        urlMod = input(
            "Order: "
            + orderName
            + " File:"
            + fileId
            + "\n"
            + "Enter y to mimic a receive failure on file - 'go' to run to end> "
        )
        # If you put go all further runs will automatically go through
        if urlMod == "go":
            debugMode = False
        if debugMode == True and urlMod == "y":
            url = (
                baseUrl
                + "/orders/"
                + orderName
                + "/latest/"
                + fileId
                + urlMod
                + "/data"
            )

    actualHeaders = {"Accept": "application/x-grib"}
    actualHeaders.update(requestHeaders)

    with requests.get(
        url, headers=actualHeaders, allow_redirects=True, stream=True
    ) as r:

        if r.url.find("--") != -1:
            if verbose: 
                print("-- found in redirect: ", r.url)

        if printUrl == True:
            print("get_order_file: ", url)
            if url != r.url:
                print("redirected to: ", r.url)

        if r.status_code != 200:

            print("ERROR: File download failed.")
            print("Headers: ",r.headers)
            print("Text: ",r.text)
            print("URL:", url)
            print("Redirected URL:",r.url)

            raise Exception("HTTP Reason and Status: " +
                            r.reason, r.status_code)

        # Record time to first byte
        ttfb = start + r.elapsed.total_seconds()

        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    return [ttfb, local_filename]


def get_files_by_run(order, runsToDownload, numFilesPerOrder):

    # Break down the files in to those needed for each run
    filesByRun = {}
    for run in runsToDownload:

        filesByRun[run] = []
        fc = 0
        for f in order["orderDetails"]["files"]:
            fileId = f["fileId"]
            if "_+" + run in fileId:
                filesByRun[run].append(fileId)
                fc += 1
                if numFilesPerOrder > 0 and fc >= numFilesPerOrder:
                    break

    return filesByRun


def download_worker():

    if taskQueue:

        while True:
            downloadTask = taskQueue.get()
            if downloadTask is None:

                break

            current_time = datetime.now().strftime("%H-%M-%S-%f")

            fileSize = 0
            errMsg = ""
            error = False
            timeToFirstByte = 0
            startTime = time.time()
            try:
                downloadResp = get_order_file(
                    downloadTask["baseUrl"],
                    downloadTask["requestHeaders"],
                    downloadTask["orderName"],
                    downloadTask["fileId"],
                    downloadTask["guidFileNames"],
                    downloadTask["folder"],
                    startTime,
                    downloadTask["backdatedDate"]
                )
                timeToFirstByte = round((downloadResp[0] - startTime), 2)
                downloadedFile = downloadResp[1]
                fileSize = os.path.getsize(downloadedFile)

            except Exception as ex:
                error = True
                errMsg = ex.args

            completeTime = time.time()
            completeDuration = round((completeTime - startTime), 2)

            if error:
                downloadTask["downloadErrorLog"].append(
                    {
                        "URL": downloadTask["baseUrl"]
                        + "/orders/"
                        + downloadTask["orderName"]
                        + "/latest/"
                        + downloadTask["fileId"]
                        + "/data",
                        "fileid": downloadTask["fileId"],
                        "currentTime": current_time,
                        "ordername": downloadTask["orderName"],
                        "folder": downloadTask["folder"],
                    }
                )
                downloadTask["responseLog"].append(
                    {
                        "order": downloadTask["orderName"],
                        "fileId": downloadTask["fileId"],
                        "error": error,
                        "fileSize": fileSize,
                        "errMsg": errMsg,
                        "time_to_first_byte": timeToFirstByte,
                        "duration": completeDuration,
                        "file": "",
                        "currentTime": current_time,
                    }
                )
                if verbose:
                    print(
                        "File: "
                        + downloadTask["fileId"]
                        + " failed "
                        + format(errMsg)
                        + "\n"
                    )
            else:
                downloadTask["responseLog"].append(
                    {
                        "order": downloadTask["orderName"],
                        "fileId": downloadTask["fileId"],
                        "error": error,
                        "fileSize": fileSize,
                        "errMsg": errMsg,
                        "time_to_first_byte": timeToFirstByte,
                        "duration": completeDuration,
                        "file": downloadedFile,
                        "currentTime": current_time,
                    }
                )

            taskQueue.task_done()


def write_failures(downloadErrorLog, fileName):

    if len(downloadErrorLog) == 0:
        return

    with open(fileName, "w") as failurefile:

        for line in downloadErrorLog:
            failurefile.write(line["URL"] + "\n")

    failurefile.close()


def write_summary(responseLog, fileName, sstartTime):

    endTime = datetime.now()

    if len(responseLog) == 0:
        return

    with open(fileName, "w", newline="") as csvfile:

        fileSizeTotal = 0
        index = 0

        csvfile.write(
            "The download of order ["
            + responseLog[0]["order"]
            + "] started at: "
            + sstartTime.strftime("%d/%m/%Y %H:%M:%S")
            + " finished at: "
            + endTime.strftime("%d/%m/%Y %H:%M:%S\n")
        )

        for row in responseLog:
            fileSizeTotal += responseLog[index]["fileSize"]
            index += 1
        csvfile.write(
            "Total Files: "
            + str(len(responseLog))
            + " Total time taken: "
            + str(round((endTime - sstartTime).total_seconds(), 2))
            + "s Total Size: "
            + str(fileSizeTotal)
            + " Workers: "
            + str(numThreads)
            + "\n"
        )
        csvfile.write("===== Detail Section =====\n")

        fieldnames = [
            "order",
            "duration",
            "time_to_first_byte",
            "fileSize",
            "fileId",
            "error",
            "errMsg",
            "file",
            "currentTime",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in responseLog:
            writer.writerow(row)
        # csvfile.write("Total Files: " + str(len(responseLog)) +  " Total time taken: " + str(round((endTime-sstartTime).total_seconds(),2)) + "s Total Size: " + str(fileSizeTotal) + "\n")
        if verbose:
            print(
                "    Total Files: "
                + str(len(responseLog))
                + " Total time taken: "
                + str(round((endTime - sstartTime).total_seconds(), 2))
                + "s Total Size: "
                + str(fileSizeTotal)
                + " Workers: "
                + str(numThreads)
                + "\n"
            )


def join_files(responseLog, joinedFileName):

    with open(joinedFileName, "wb") as outfile:
        for f in responseLog:

            with open(f["file"], "rb") as infile:
                while True:
                    chunk = infile.read(8192)
                    if not chunk:
                        break
                    outfile.write(chunk)
            os.remove(f["file"])


def get_my_orders(baseUrl, requestHeaders):

    ordHeaders = {"Accept": "application/json"}
    ordHeaders.update(requestHeaders)

    ordurl = baseUrl + "/orders?detail=MINIMAL"
    ordr = requests.get(ordurl, headers=ordHeaders)
    if printUrl == True:
        print("get_my_orders: ", ordurl)
        if ordurl != ordr.url:
            print("redirected to: ", ordr.url)

    if ordr.status_code != 200:
        print("ERROR:  Unable to get my orders list. Status code: ", ordr.status_code)            
        print("Headers: ",ordr.headers)
        print("Text: ",ordr.text)
        print("URL:", ordurl)
        sys.exit(1)
    orddetails = ordr.json()

    return orddetails


def get_latest_run(modelID, orderName, modelRuns):

    latestRun = modelRuns[modelID][:2]
    latestDate = modelRuns[modelID][3:]
    stamp = latestDate[:10] + ":" + latestRun

    # Determine increment to add to get missed runs
    if "uk" in modelID:
        runIncrement = 1
        maxRuns = 24 
    else:
        runIncrement = 6
        maxRuns = 4


    if not os.path.exists(baseFolder + LATEST_FOLDER + "/" + orderName + ".txt"):
        # File not there - so write it and return latest run
        # No attempt to get backdated runs
        rf = open(baseFolder + LATEST_FOLDER + "/" + orderName + ".txt", "w")
        rf.write(stamp)
        rf.close()
    else:
        # Open the file and retrieve the last run
        rf = open(baseFolder + LATEST_FOLDER + "/" + orderName + ".txt", "r")
        laststamp = rf.read()
        rf.close()
        # Check to see if the latest is later than the last run
        if stamp > laststamp:
            rf = open(baseFolder + LATEST_FOLDER +
                      "/" + orderName + ".txt", "w")
            rf.write(stamp)
            rf.close()
            # Now work out what runs we've missed
            stampDate = datetime.strptime(stamp,"%Y-%m-%d:%H")
            laststampDate = datetime.strptime(laststamp,"%Y-%m-%d:%H")
            latestRun = ""
            # Need to check we aren't asking for too many dates
            while laststampDate < stampDate:
                laststampDate = laststampDate + timedelta(hours=runIncrement)
                newHour = laststampDate.strftime("%H")
                if latestRun == "":
                    latestRun = newHour
                else:
                    latestRun = latestRun + "," + newHour
            # OK if it was a long time ago this could have led to too many runs
            latestRun = latestRun[((-1)*maxRuns*3)+1:]

        else:
            latestRun = "done" + ":" + latestRun

    return latestRun


def get_model_runs(baseUrl, requestHeaders, modelList):

    modelRuns = {}
    runHeaders = {"Accept": "application/json"}
    runHeaders.update(requestHeaders)

    for model in modelList:
        requrl = baseUrl + "/runs/" + model + "?sort=RUNDATETIME"

        for loop in range(retryCount):

            reqr = requests.get(requrl, headers=runHeaders)

            if printUrl == True:
                print("get_model_runs: ", requrl)
                if requrl != reqr.url:
                    print("redirected to: ", reqr.url)

            if reqr.status_code != 200:
                print(
                    "ERROR:  Unable to get latest run for model: "
                    + model
                    + " status code: ",
                    reqr.status_code,
                )
                if loop != (retryCount - 1):
                    time.sleep(10)
                    continue
                else:
                    print("ERROR:  Ran out of retries to get latest run for model: ")
                    break

            rundetails = reqr.json()
            rawlatest = rundetails["completeRuns"]
            modelRuns[model] = rawlatest[0]["run"] + \
                ":" + rawlatest[0]["runDateTime"]
            break

    return modelRuns


def run_wanted(allorders, ordername, latestrun):

    result = False
    for ords in allorders["orders"]:
        if ords["orderId"] == ordername:
            if latestrun in ords["requiredLatestRuns"]:
                result = True
            else:
                result = False

    return result


def order_exists(allorders, ordername):

    result = False
    for ords in allorders["orders"]:
        if ords["orderId"] == ordername:
            result = True
            break

    return result


def get_model_from_order(allorders, ordername):

    result = "Not found"
    for ords in allorders["orders"]:
        if ords["orderId"] == ordername:
            result = ords["modelId"]
            break

    return result


if __name__ == "__main__":

    ROOT_FOLDER = "downloaded"
    LATEST_FOLDER = "latest"
    RESULTS_FOLDER = "results"
    FAILURES_FOLDER = "failures"

    parser = argparse.ArgumentParser(
        description="Download all the files for one or more order from the CDA delivery service."
    )
    parser.add_argument(
        "-u",
        "--url",
        action="store",
        dest="baseUrl",
        default=BASE_URL,
        help="Base URL used to access Weather DataHub API. Defaults to https://api-metoffice.apiconnect.ibmcloud.com/metoffice/production/1.0.0.",
    )
    parser.add_argument(
        "-c",
        "--client",
        action="store",
        dest="clientId",
        default="",
        help="REQUIRED: Client ID of your WDH Application",
    )
    parser.add_argument(
        "-s",
        "--secret",
        action="store",
        dest="secret",
        default="",
        help="REQUIRED: Your WDH API Gateway secret",
    )
    parser.add_argument(
        "-o",
        "--orders",
        action="store",
        dest="ordersToDownload",
        default="default_order",
        help="REQUIRED: Comma separated list of order names to download.",
    )
    parser.add_argument(
        "-r",
        "--runs",
        action="store",
        dest="orderRuns",
        default="00,06,12,18",
        help="Comma separated list of runs to download or -r latest to get latest run.",
    )
    parser.add_argument(
        "-w",
        "--workers",
        action="store",
        dest="workers",
        default=4,
        type=int,
        help="Number of workers used to perform downloads. Defaults to 4.",
    )
    parser.add_argument(
        "-j",
        "--join",
        action="store_true",
        dest="joinFiles",
        default=False,
        help="If present, all the downloaded files will be concatenated together.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="Verbose mode.",
    )
    parser.add_argument(
        "-d",
        "--folderdate",
        action="store_true",
        dest="folderdate",
        default=False,
        help="Add the YYYYMMDDhhmm_RR to the download folder.",
    )
    parser.add_argument(
        "-l",
        "--location",
        action="store",
        dest="location",
        default="",
        help="The base folder to store files",
    )
    parser.add_argument(
        "-m",
        "--modellist",
        action="store",
        dest="modellist",
        default=MODEL_LIST,
        help="Pass the ist of models to support.",
    )
    parser.add_argument(
        "-a",
        "--retry",
        action="store_true",
        dest="retry",
        default=False,
        help="Retry again the failures automatically.",
    )
    parser.add_argument(
        "-p",
        "--retryperiod",
        action="store",
        dest="retryperiod",
        default="30",
        help="Retry delay in seconds.",
    )
    parser.add_argument(
        "-x",
        "--printurl",
        action="store_true",
        dest="printurl",
        default=False,
        help="Print all accessed URLs and redirects",
    )
    parser.add_argument(
        "-z",
        "--debug",
        action="store_true",
        dest="debugmode",
        default=False,
        help="Switch to debug mode.",
    )
    parser.add_argument(
        "-k",
        "--apikey",
        action="store",
        dest="apikey",
        default="",
        help="Use direct API Key when not via APIM.",
    )
    parser.add_argument(
        "-b",
        "--backdated",
        action="store",
        dest="backdatedDate",
        default="",
        help="OPTIONAL: Date in YYYYMMDD to explicitly attempt to get files from.",
    )

    args = parser.parse_args()

    baseUrl = args.baseUrl
    clientId = args.clientId
    secret = args.secret
    orderRuns = args.orderRuns
    useEnhancedApi = True
    verbose = args.verbose
    folderdate = args.folderdate
    numThreads = args.workers
    myModelList = args.modellist
    joinFiles = args.joinFiles
    retry = args.retry
    retryperiod = args.retryperiod
    debugMode = args.debugmode
    baseFolder = args.location
    apikey = args.apikey
    backdatedDate = args.backdatedDate

    printUrl = args.printurl

    thereWereErrors = False

    if debugMode == True:
        print("WARNING: As we are in debug mode setting workers to one.")
        numThreads = 1

    # Check for backdatedDate and latest - incompatible

    if backdatedDate != "" and orderRuns == "latest":
        print("ERROR: You cannot request the latest run and pass a specific date to download.")
        sys.exit()

    if args.ordersToDownload == "":
        print("ERROR: You must pass an orders list to download.")
        sys.exit()
    else:
        ordersToDownload = args.ordersToDownload.lower().split(",")

    joinFiles = False
    numFilesPerOrder = 0
    guidFileNames = False

    # Client ID and Sectet must be supplied
    if (clientId == "" or secret == "") and apikey == "":
        print("ERROR: IBM client and secret must be supplied.")
        sys.exit()

    if apikey == "":
        requestHeaders = {"x-ibm-client-id": clientId,
                          "x-ibm-client-secret": secret}
    else:
        requestHeaders = {"x-api-key": apikey}

    if baseFolder != "":
        try:
            if baseFolder[-1] != "/":
                baseFolder = baseFolder + "/"
            os.makedirs(baseFolder, exist_ok=True)
        except OSError as error:
            print("ERROR: Base folder", baseFolder,
                  "cannot be accessed or created.")
            sys.exit()

    os.makedirs(baseFolder + ROOT_FOLDER, exist_ok=True)
    os.makedirs(baseFolder + LATEST_FOLDER, exist_ok=True)
    os.makedirs(baseFolder + RESULTS_FOLDER, exist_ok=True)
    os.makedirs(baseFolder + FAILURES_FOLDER, exist_ok=True)

    if verbose:
        print("Download Orders")
        print("===============")

    ordersfound = False

    # Get my orders for future reference
    myOrders = get_my_orders(baseUrl, requestHeaders)

    if len(myOrders["orders"]) == 0:
        print(
            "WARNING: You have no orders active on Weather DataHub.  Please confirm some orders and try again."
        )
        sys.exit()

    # For each of the orders to download get the model and add to my model list
    myModelList = []
    for orderName in ordersToDownload:
        newModel = get_model_from_order(myOrders, orderName)
        if newModel not in myModelList:
            myModelList.append(newModel)
    if verbose == True:
        print(
            "From the orders to process we have the following model list from active orders: ",
            myModelList,
        )

    if myModelList == [] or myModelList == ["Not found"]:
        print(
            "ERROR: No models could be extracted from the orders to process: "
            + str(ordersToDownload)
        )
        sys.exit()

    myModelRuns = get_model_runs(baseUrl, requestHeaders, myModelList)

    retryManifest = []

    # Total number of files downloaded

    totalFiles = 0
    finalRuns = []
    myTimeStamp = datetime.now().strftime("%d-%b-%Y-%H-%M-%S")

    # Process selected orders, generating tasks for the worker to actually download the file.
    for orderName in ordersToDownload:

        initTime = datetime.now()

        responseLog = []
        downloadErrorLog = []
        if verbose:
            print("Processing: " + orderName)
        if not order_exists(myOrders, orderName):
            print(
                "ERROR: You've asked for an order called: "
                + orderName
                + " which doesn't appear in the list of active orders."
            )
            continue
        if orderRuns == "":
            runsToDownload = ["00", "06", "12", "18"]
        else:
            if orderRuns == "latest":
                modelToGet = get_model_from_order(myOrders, orderName)
                if modelToGet not in myModelList:
                    print(
                        "ERROR: No idea what model: "
                        + modelToGet
                        + " is so terminating!"
                    )
                    sys.exit(7)
                runsToDownload = get_latest_run(
                    modelToGet, orderName, myModelRuns)
                if runsToDownload[:4] == "done":
                    if verbose:
                        print(
                            "We have done this latest run "
                            + runsToDownload[5:]
                            + " already!"
                        )
                    continue
                # Do I want these runs?
                finalRuns = []
                runsToCheck = runsToDownload.split(",")
                for checkRun in runsToCheck:

                    runWanted = run_wanted(myOrders, orderName, checkRun)
                    if runWanted and verbose:
                        print("This run " + checkRun + " is wanted.")
                        finalRuns.append(checkRun)
                    else:
                        if verbose:
                            print("This run " + checkRun + " is not wanted")
                        continue
                
                runsToDownload = finalRuns
                

            else:
                runsToDownload = orderRuns.split(",")
                finalRuns = []
                # Ensure only runs wanted are asked for
                for checkRun in runsToDownload:
                    if run_wanted(myOrders, orderName, checkRun):
                        finalRuns.append(checkRun)
                    else:
                        print(
                            "WARNING: The run "
                            + checkRun
                            + " has been asked for but doesn't appear in the order "
                            + orderName
                        )
                runsToDownload = finalRuns

        if len(finalRuns) == 0:
            print(
                "WARNING: No runs for order "
                + orderName
                + "were found.  Don't expect any data."
            )
            continue

        order = get_order_details(
            baseUrl, requestHeaders, orderName, useEnhancedApi, runsToDownload
        )
        if order != None:

            # Create queue and threads for processing downloads
            taskQueue = queue.Queue()
            taskThreads = []
            for i in range(numThreads):
                t = threading.Thread(target=download_worker)
                taskThreads.append(t)
            # End of set up threads
            ordersfound = True

            # Break down the files in to those needed for each run
            filesByRun = get_files_by_run(
                order, runsToDownload, numFilesPerOrder)

            #pprint.pprint(filesByRun)

            # Now queue up tasks to down load each file
            for run in runsToDownload:

                if folderdate == True:
                    folder = (
                        baseFolder
                        + ROOT_FOLDER
                        + "/"
                        + initTime.strftime("%Y%m%d%H%M_")
                        + run
                        + "/"
                        + orderName
                        + "_"
                        + run
                    )
                else:
                    folder = baseFolder + ROOT_FOLDER + "/" + orderName + "_" + run

                os.makedirs(folder, exist_ok=True)
                for fileId in filesByRun[run]:
                    downloadTask = {
                        "baseUrl": baseUrl,
                        "requestHeaders": requestHeaders,
                        "orderName": orderName,
                        "fileId": fileId,
                        "guidFileNames": guidFileNames,
                        "folder": folder,
                        "responseLog": responseLog,
                        "downloadErrorLog": downloadErrorLog,
                        "backdatedDate": backdatedDate,
                    }
                    taskQueue.put(downloadTask)

        # Start the worker threads
        if ordersfound == False:
            print(
                "WARNING: No orders or runs were found from this list: ",
                ordersToDownload,
            )
            continue

        if verbose:
            print("    Starting downloads")
        for t in taskThreads:
            t.start()

        # Wait for all the queued scenarios to be processed
        taskQueue.join()

        # Stop all the threads
        for i in range(numThreads):
            taskQueue.put(None)

        for t in taskThreads:
            t.join()

        # Write out the summary CSV file
        summaryFileName = (
            baseFolder + "results/summary-" + orderName + "-" + myTimeStamp + ".txt"
        )
        failuresFileName = (
            baseFolder + "failures/summary-" + orderName + "-" + myTimeStamp + ".txt"
        )

        if len(downloadErrorLog) > 0:
            write_failures(downloadErrorLog, failuresFileName)
            print(
                "WARNING: there were",
                len(downloadErrorLog),
                "detected download failures\nDetails in file: " + failuresFileName,
            )
            if retry:
                retryManifest = retryManifest + downloadErrorLog

        write_summary(responseLog, summaryFileName, initTime)
        totalFiles = totalFiles + len(responseLog)

        if verbose and len(responseLog) > 0:
            print("    Created summary: " + summaryFileName)

    # End of order processing loop

    if verbose:
        print("All file downloads have been attempted.")

    # Do we have any retries we want to do
    if retry and len(retryManifest) > 0:
        if verbose:
            print("We have files to retry")
        totalFailures = len(retryManifest)
        failureRate = (totalFailures / totalFiles) * 100.00
        if verbose:
            print("The failure rate is", failureRate, "percent.")

        if totalFailures > 100:
            print(
                "ERROR: total failures of",
                totalFailures,
                "is more than the 100 limit can't recover.",
            )
            sys.exit(2)

        if totalFailures == totalFiles:
            print(
                "ERROR: Everything failed for all",
                totalFiles,
                "files - terminating program.",
            )
            sys.exit(3)

        if failureRate > 50.0 and totalFailures > 50:
            print(
                "ERROR: failure rate > 50 percent and more than 20 failures - terminating."
            )
            sys.exit(4)

        # I can now retry
        # Wait for the asked time
        if verbose:
            print("Wait of", retryperiod, "starting.")
        time.sleep(int(retryperiod))
        if verbose:
            print("Wait of", retryperiod, "ended.")
        # Wait ended

        actualHeaders = {"Accept": "application/x-grib"}
        actualHeaders.update(requestHeaders)
        stillInError = []
        deleteFile = False

        for retryFile in retryManifest:
            if verbose:
                print("Re-trying " + retryFile["fileid"])
            startTime = time.time()
            failuresFileName = (
                baseFolder
                + "failures/summary-"
                + orderName
                + "-"
                + myTimeStamp
                + ".txt"
            )
            summaryFileName = (
                baseFolder + "results/summary-" + orderName + "-" + myTimeStamp + ".txt"
            )
            if deleteFile == False:
                if os.path.isfile(failuresFileName):
                    os.remove(failuresFileName)
                deleteFile = True

            error = False

            try:
                if apikey != "":
                    requestHeaders = {"x-api-key": apikey}

                if verbose:
                    print(
                        "Retrying",
                        baseUrl,
                        retryFile["ordername"],
                        retryFile["fileid"],
                        retryFile["folder"],
                    )
                downloadResp = get_order_file(
                    baseUrl,
                    requestHeaders,
                    retryFile["ordername"],
                    retryFile["fileid"],
                    False,
                    retryFile["folder"],
                    startTime,
                    backdatedDate
                )
                fileSize = os.path.getsize(downloadResp[1])

            except Exception as ex:
                error = True
                errMsg = ex.args
                status = ex.args[1]

            if not error:
                with open(summaryFileName, "a") as sumfile:
                    sumfile.write(
                        retryFile["ordername"]
                        + ",0,0,0,"
                        + retryFile["fileid"]
                        + ",False,RETRY-OK,"
                        + downloadResp[1]
                        + ","
                        + datetime.now().strftime("%H-%M-%S-%f")
                        + "\n"
                    )
                sumfile.close()

            else:
                with open(failuresFileName, "a") as errfile:
                    errfile.write(
                        "File "
                        + retryFile["fileid"]
                        + " FAILED on retry. errMsg: "
                        + format(errMsg)
                        + " status: "
                        + str(status)
                        + "\n"
                    )
                errfile.close()
                thereWereErrors=True
                stillInError.append(retryFile.copy())
    
    if thereWereErrors == True:
        print("ERROR: something remains in error.")
        sys.exit(10)

# End of python program.