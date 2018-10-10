from sqlalchemy import create_engine
import yaml

def load_config(fn='./config.yml'):
    with open(fn, 'r') as fin:
        return yaml.load(fin)

def engine():
    config = load_config()
    return create_engine(f"postgresql://{config.get('user', '')}:{config.get('passwd', 'mysecretpassword')}@localhost:5432/mydatabase")