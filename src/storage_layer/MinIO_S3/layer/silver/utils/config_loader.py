import yaml

def load_config_yaml(yaml_path):
    with open(yaml_path, "r", encoding="utf-8") as file:
        # Sử dụng safe_load để đảm bảo an toàn bảo mật
        config = yaml.safe_load(file)
    
    return config