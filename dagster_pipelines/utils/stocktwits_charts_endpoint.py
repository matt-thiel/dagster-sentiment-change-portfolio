import urllib.request
import base64
import gzip
import shutil
import dotenv
import os

dotenv.load_dotenv()

username = os.environ["STOCKTWITS_USERNAME"]
password = os.environ["STOCKTWITS_PASSWORD"]

credentials = f"{username}:{password}"
print(credentials)
encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
print(encoded_credentials)

zoom = "1D"
output_file = "output.json"

url = f"https://api-gw-prd.stocktwits.com/api-middleware/external/sentiment/v2/charts?zoom={zoom}"

# urllib handles redirects (including 307) automatically
with urllib.request.urlopen(urllib.request.Request(url, headers={"Authorization": f"Basic {encoded_credentials}"})) as response:
    compressed_file = output_file + ".gz"
    with open(compressed_file, "wb") as f:
        shutil.copyfileobj(response, f)

# Decompress
with gzip.open(compressed_file, "rb") as f_in:
    with open(output_file, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

print(f"✅ File saved to {output_file}")
