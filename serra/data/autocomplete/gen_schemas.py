import json

def read_json_file(file_path):
    # Opening JSON file
    data = {}
    with open(file_path) as json_file:
        data = json.load(json_file)

    return data

def write_to_file(data_dict, output_file):
    try:
        with open(output_file, 'w') as file:
            json.dump(data_dict, file, indent=4, sort_keys=True)
        print(f"Dictionary has been pretty printed to '{output_file}' successfully.")
    except Exception as e:
        print(f"Error occurred while writing to file: {e}")

SPEC_PATH = "./inputs/specs.json"
SCHEMA_TEMPLATE = "./templates/schema_template.json"
BLOCK_TEMPLATE = "./templates/transformer_template.json"
OUTPUT_DIR = "./output"

specs = read_json_file(SPEC_PATH)
schema_template = read_json_file(SCHEMA_TEMPLATE)
transformer_template = read_json_file("./templates/transformer_template.json")

# Create the main schema json
for class_name in specs:
    new_property = {
            "type": "object",
            "properties": {
                f"{class_name}" : { "$ref": f"./{class_name}.json" },
                "tests": {"type": "array"}
                },
            "additionalProperties": False
          }

    schema_template["patternProperties"]["^(?!debug$).*"]['oneOf'].append(new_property)

# Create the schemas for each class
for class_name in specs:
    class_schema = read_json_file(BLOCK_TEMPLATE)

    for property,property_type in specs[class_name].items():
        class_schema['properties'][property] = {"type": property_type}
    write_to_file(class_schema, f"{OUTPUT_DIR}/{class_name}.json")

write_to_file(schema_template,f"{OUTPUT_DIR}/serra_yaml_schema.json")
