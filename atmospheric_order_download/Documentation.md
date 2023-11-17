## Order Download

This is a simple set of utilities to download customer orders using the API and to report errors.

It is available currently in Python.

It can be configured to use multiple worker threads to allow a degree of concurrency when downloading the files.

There are options to support getting the latest run, preventing retrieval of runs already processed and retries.

## Python instructions

### Installation

Ensure you have python 3.10 minimum and pip installed, check with
```
py --version
```
and:

```
pip --version
```

### If you wish to run in a virtual environment

Navigate to the root of the repository and create the virtual environment:
```
py -m venv env
```
And activate the virtual environment:

```
.\env\Scripts\activate
```

Finally, to install the required packages run this command in the root of the order_download project:
```
pip install requests
```

To deactivate the virtual environment, simple run:
```
.\env\Scripts\deactivate
```

## Running

Assuming you have completed the install of the requests package.

To run:
```
py cda_download.py {arguments}
```
Client ID, the secret and orders to download are the only mandatory parameters.

The utility will follow any re-directs and thus supports redirected delivery.

## Command line options

| Option          | - | Description                           | Example of use                                                    | Default |
| --------------- | - |--------------------------------- | ---------------------------------------------------------------|-------- |
| --url           | -u| Service base URL                 | --url https://api-metoffice.apiconnect.ibmcloud.com/metoffice/production/1.0.0 |         |  
| --client        | -c| WDH client id key                 | --client xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx    |         |  
| --secret        | -s| WDH secret key                    | --secret xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx    |         |  
| --orders        | -o| List of orders name to download  | --orders p3_pp_euro,p3_pp_global                  |         |  
| --runs          | -r| List of runs to download         | --runs 00,12 or latest                            | 0,6,12,18 |  
| --workers       | -w| Number of worker threads         | --workers 2                                       | 4       |  
| --join          | -j| Join downloaded files together   | --join                                            | False   | 
| --verbose       | -v| Print extra status messages      | --verbose                                         | False   | 
| --folderdate    | -d| Add date/time/run to folder      | --folderdate                                      | False   | 
| --location      | -l| The base folder to store files   | --location C:\Data                                |         | 
| --modellist     | -m| Pass the list of models to use   | --modellist mo-global,m-uk-latlon                 |         | 
| --retry         | -a| Retry failures from each order   | --retry                                           | False   | 
| --retryperiod   | -p| Seconds to wait for retry        | --retryperiod 20                                  | 30      | 
| --debug         | -z| Put into debug mode              | --debug                                           | False   | 
| --perfmode      | -y| Turn on API performance checking | --perfmode                                        | False   | 
| --perftime      | -t| Length of MDDA calls to report   | --perftime 3                                      | 10      | 
| --printurl      | -x| Print URLs as accessed/redirected | --printurl                                       | False   | 
| --savefilelist  | -f| Save the file list               | --savefilelist                                    | False   |
| --verifyssloff  | -q| Turn off verify SSL in requests  | --verifyssloff                                    | False   | 
| --fillgaps      | -g| Only download the gaps in files  | --fillgaps                                        | False   |



## Some guidance on use

```
--url 
```

The default is to use the production URL so this does not need to be passed unless a different URL is being used.

```
--runs latest
```

The --runs latest will return the latest set of files available for a particular order.  So for example if you have a global order set for runs 00 and 12 (i.e. the full runs) calling the program with the latest parameter will ensure you only get what you want, once, despite how ofted you call the program.  The latest/ folder will store the latest files for every order - these can be edited with a text editor (or deleted) to re-enable a run.

If you call the download once a day all missing runs asked for on the order will be retrieved to catch up to a consistent position.

Using --runs latest downloads the latest data run available. If this run is not included on your order then the latest run will not be downloaded. Once the latest data run for your order becomes available this can be downloaded using latest runs.
```
--retry
```

After the initial run - any files that failed to download are added to a retry list.  If this list is too long (>100) or the fail percentage is greater than 50% and also more than 20 need to be downloaded or all files failed then the program terminates.  This is to avoid excess errors as these conditions indicate something major is likely to be wrong.

Re-retrieves are attempted after the delay passed (--retryperiod) or the default 300 seconds.  The list of files retrieved second time around is added to the results/ text list and anything left still unreceived can be found in the failures/ folder.


```
--location
```

This is the base location where all folders to store data and reports are stored.  If not set the directory from where the program is invoked is used.


```
--folderdate
```

This creates an additional folder in the downloaded/ area called YYYYMMDDhhmm_RR - where the RR is the run.  Within there is the normal order_RR folder.

```
--debug
```
 
This will allow the user to interactively fail a file receive to test the retry functionality and will be used for other debug style functions as needed. 
Limits the workers to one and you can carry on at any point by entering 'go'.

```
--perfmode
```

This turns on a performance monitoring mode that allows the user to see the times various API calls take.  And can report on particularly slow downloads ot data files.

```
--perftime
```

Sets the download time for individual files after which the time taken is reported.  Useful for diagnosing network issues.

```
--printurl
```

Diplays the URLs called and any redirects.  

```
--savefilelist
```

Save the filelist in a file called ordername_{date_time}.json.  Uses the filelists folder.

```
--fillgaps
```

If an incomplete run has been downloaded, resulting in some missing data, the script can be rerun using the fillGaps flag, so only the missing data will be downloaded. This will prevent you using an excess of your data allowance.