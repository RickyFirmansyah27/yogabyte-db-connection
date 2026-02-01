import os
from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    
    # Path to the root certificate, relative to this file or absolute
    DB_ROOT_CERT: str = "root.crt" 

    class Config:
        env_file = os.path.join(os.path.dirname(__file__), '../.env')
        env_file_encoding = 'utf-8'
        extra = "ignore" 

@lru_cache()
def get_settings():
    return Settings()
