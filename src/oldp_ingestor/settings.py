import os

from dotenv import load_dotenv

load_dotenv()

OLDP_API_URL = os.environ.get("OLDP_API_URL", "")
OLDP_API_TOKEN = os.environ.get("OLDP_API_TOKEN", "")
OLDP_API_HTTP_AUTH = os.environ.get("OLDP_API_HTTP_AUTH", "")  # format: user:password

EURLEX_USER = os.environ.get("EURLEX_USER", "")
EURLEX_PASSWORD = os.environ.get("EURLEX_PASSWORD", "")
