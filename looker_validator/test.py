import looker_sdk
import os

# Set environment variables
os.environ["LOOKERSDK_BASE_URL"] = "https://looker.mediamarktsaturn.com"
os.environ["LOOKERSDK_CLIENT_ID"] = "pwcMFRh3C6K5D6XHthMt"
os.environ["LOOKERSDK_CLIENT_SECRET"] = "6h7PsqG7Xg73SRK8288Kkn2b"

# Initialize SDK
sdk = looker_sdk.init40()

# Test connection
try:
    me = sdk.me()
    print(f"Connection successful! User: {me.display_name}")
except Exception as e:
    print(f"Connection failed: {e}")
