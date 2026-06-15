import os

from dotenv import load_dotenv

load_dotenv()

OLDP_API_URL = os.environ.get("OLDP_API_URL", "")
OLDP_API_TOKEN = os.environ.get("OLDP_API_TOKEN", "")
OLDP_API_HTTP_AUTH = os.environ.get("OLDP_API_HTTP_AUTH", "")  # format: user:password

# Read-only OLDP instance used by `lookup providers` to resolve each
# provider's declared court_filter against the live catalogue. Public
# endpoints only — never sends auth headers — so it can point at the
# prod instance even when ``OLDP_API_URL`` targets a local dev box.
OLDP_PROD_API_URL = os.environ.get("OLDP_PROD_API_URL", "https://de.openlegaldata.io")

EURLEX_USER = os.environ.get("EURLEX_USER", "")
EURLEX_PASSWORD = os.environ.get("EURLEX_PASSWORD", "")
