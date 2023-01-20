## Order Download

This is a simple set of utilities to download customer orders using the API and to report errors.

It is available currently in Python.

It can be configured to use multiple worker threads to allow a degree of concurrency when downloading the files.

There are options to support getting the latest run, preventing retrieval of runs already processed and retries.

## Python instructions

### Installation

Ensure you have python 3.7 minimum and pip installed, check with
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
py ss_download.py {arguments}
```
Client ID, the secret, latitude and longitude are the only mandatory parameters.

## Command line options

| Option           | - | Description                           | Example of use                                                    | Default |
| ---------------- | - |--------------------------------- | ---------------------------------------------------------------|-------- |
| --timesteps      | -t| Frequency of timesteps            | --timesteps daily| hourly         |  
| --client         | -c| WDH client id key                 | --client xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx    |         |  
| --secret         | -s| WDH secret key                    | --secret xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx    |         |  
| --latitude       | -y| Latitude of the location for the forecast  | --latitude 50.7                  |         |  
| --longitude      | -x| Longitude of the location for the forecast         | --longitude -3.52                            |    |  
| --metadata        | -m| Exclude parameter metadata         | --metadata true                                       | False       |  
| --name           | -n| Include the name of the forecast location   | --name false                                            | True   | 

## Some guidance on use

```
--timesteps 
```

There are three frequencies of timesteps available - hourly, three-hourly or daily.

```
--latitude
```

Provide the latitude of the location for the forecast. This should be a valid latitude, between -90 and 90. 

```
--longitude
```

Provide the longitude of the location for the forecast. This should be a valid longitude, between -180 and 180. 


