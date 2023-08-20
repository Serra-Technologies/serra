import yaml

class SerraProfile:

    def __init__(self, config):
        self.config = config

    @staticmethod
    def from_string(config_string):
        config = yaml.safe_load(config_string)
        return SerraProfile(config)

    @staticmethod
    def from_yaml_path(config_path):
        with open(config_path, 'r') as stream:
            config = yaml.safe_load(stream)
        return SerraProfile(config)

    @property
    def aws_access_key_id(self):
        return self.config.get("AWS_ACCESS_KEY_ID")
    
    @property
    def aws_secret_access_key(self):
        return self.config.get("AWS_SECRET_ACCESS_KEY")
    
    @property
    def aws_config_bucket(self):
        return self.config.get("AWS_CONFIG_BUCKET")
    
    @property
    def databricks_host(self):
        return self.config.get("DATABRICKS_HOST")
    
    @property
    def databricks_token(self):
        return self.config.get("DATABRICKS_TOKEN")
    
    @property
    def databricks_cluster_id(self):
        return self.config.get("DATABRICKS_CLUSTER_ID")
    
    @property
    def snowflake_account(self):
        return self.config.get("SNOWFLAKE")

