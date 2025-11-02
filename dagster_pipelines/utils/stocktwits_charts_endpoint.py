"""
Sample script to download new ST endpoint.
"""

import urllib.request
import base64
import gzip
import shutil
import os
import dotenv


dotenv.load_dotenv()

username = os.environ["STOCKTWITS_USERNAME"]
password = os.environ["STOCKTWITS_PASSWORD"]

credentials = f"{username}:{password}"
print(credentials)
encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
print(encoded_credentials)

ZOOM = "1D"
OUTPUT_FILE = "output.json"

url = f"https://api-gw-prd.stocktwits.com/api-middleware/external/sentiment/v2/charts?ZOOM={ZOOM}"

# urllib handles redirects (including 307) automatically
with urllib.request.urlopen(
    urllib.request.Request(
        url, headers={"Authorization": f"Basic {encoded_credentials}"}
    )
) as response:
    COMPRESSED_FILE = OUTPUT_FILE + ".gz"
    with open(COMPRESSED_FILE, "wb") as f:
        shutil.copyfileobj(response, f)

# Decompress
with gzip.open(COMPRESSED_FILE, "rb") as f_in:
    with open(OUTPUT_FILE, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

print(f"✅ File saved to {OUTPUT_FILE}")
