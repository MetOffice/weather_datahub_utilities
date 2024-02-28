## Order Download

This is a simple set of utilities to retrieve data using the API and to report errors.

It is available currently in Python.

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

### Collections
To run:
```
py bpf_feature_collections.py {arguments}
```
Client API key is the only mandatory parameter.
#### Command line options
| Option      | -  | Description        | Example of use                                | Default |
|-------------|----|--------------------|-----------------------------------------------|---------|
| --apikey    | -k | WDH client API key | --apikey xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx |         |  

### Locations
To run:
```
py bpf_feature_locations.py {arguments}
```
Client API key and a collection ID are the only mandatory parameters.
#### Command line options
| Option       | -   | Description         | Example of use                                 | Default |
|--------------|-----|---------------------|------------------------------------------------|---------|
| --collection | -c  | Collection ID       | --collection improver-percentiles-spot-global  |         |  
| --apikey     | -k  | WDH client API key  | --apikey xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx  |         |  

### Forecast Data
To run:
```
py bpf_feature_data_download.py {arguments}
```
Client API key, collection ID, and a location ID are the only mandatory parameters.
#### Command line options
| Option       | -   | Description        | Example of use                                | Default |
|--------------|-----|--------------------|-----------------------------------------------|---------|
| --collection | -c  | Collection ID      | --collection improver-percentiles-spot-global |         |  
| --apikey     | -k  | WDH client API key | --apikey xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx |         |  
| --location   | -l  | Location ID        | --location 0000046                            |         |

### Some guidance on use
```
--collections
```
There are currently four collections available which can be found using _bpf_feature_collections.py_.
```
--locations
```
The locations available for each collection can be found using _bpf_feature_locations.py_.