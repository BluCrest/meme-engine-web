import os
import yaml
from pathlib import Path
from dotenv import load_dotenv

def load_config(config_path: str = None) -> dict:
    """Load configuration from YAML file with env var substitution"""
    if config_path is None:
        config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"
    
    # Load .env file
    env_path = Path(__file__).parent.parent.parent / "config" / ".env"
    load_dotenv(env_path)
    
    with open(config_path, 'r') as f:
        raw = f.read()
    
    # Substitute environment variables ${VAR_NAME}
    import re
    def replace_env(match):
        var_name = match.group(1)
        return os.getenv(var_name, match.group(0))
    
    substituted = re.sub(r'\$\{(\w+)\}', replace_env, raw)
    return yaml.safe_load(substituted)
