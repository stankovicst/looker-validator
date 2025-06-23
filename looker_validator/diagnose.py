import asyncio
import aiohttp
import sys

async def test_connection():
    print("Testing direct connection with aiohttp...")
    
    base_url = "https://looker.xxx.com"
    client_id = "xxx"
    client_secret = "xxx"
    
    # Test different URL formats
    urls_to_test = [
        f"{base_url}/api/4.0/login",
        f"{base_url}/api/4.0/login/",
        f"{base_url}/looker/api/4.0/login",
        f"{base_url}:443/api/4.0/login",
    ]
    
    async with aiohttp.ClientSession() as session:
        for url in urls_to_test:
            print(f"\nTrying URL: {url}")
            try:
                # Match curl's exact format
                data = {
                    "client_id": client_id,
                    "client_secret": client_secret
                }
                
                async with session.post(url, data=data, timeout=30) as response:
                    print(f"  Status: {response.status}")
                    if response.status == 200:
                        result = await response.json()
                        print(f"  Success! Got token: {result['access_token'][:10]}...")
                    else:
                        try:
                            error_text = await response.text()
                            print(f"  Error response: {error_text[:100]}...")
                        except:
                            print("  Could not read error response")
            except Exception as e:
                print(f"  Exception: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_connection())
