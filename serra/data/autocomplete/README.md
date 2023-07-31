# Setup autocomplete for VSCode
## Step 1: Install redhat.vscode-yaml
Link: https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml

## Step 2: Get the path to the serra_yaml_schema.json.


```
readlink -f ./output/serra_yaml_schema.json

/path/to/serra_yaml_schema.json
```

## Step 3: Configure VSCode YAML settings
Inside settings.json (https://code.visualstudio.com/docs/getstarted/settings#_settingsjson):
```
    "yaml.schemas": {
        "file:///path/to/serra_yaml_schema.json": ["jobs/*.yml"]
    }
    "yaml.format.enable": false
```

Now any files createed with the extension ```.serra.yaml``` should show proper autocomplete for the Serra framework.

# Usage
Hit ctl+space when writing the yaml file to get suggestions.


# Modifying autocomplete
These scripts are set up to be easy to set up autocomplete for Serra config files.
Inside the inputs directory, you can modify the specs.json file to specify the parameters that each transformer, reader, or writer takes.

##  Generate JSON Schemas
```python3 gen_schemas.py```


## Read more about JSON Schema
https://json-schema.org/

