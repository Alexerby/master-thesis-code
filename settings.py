
def load_config(config_file='config.json'):
    with open(config_file, 'r') as f:
        return json.load(f)
