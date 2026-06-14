import requests

PATH = "http://localhost:8081/"

def shorten_anon(url: str, expires_in_hours: int) -> dict:
    """
    Shortens a URL using the anonymous endpoint.
    """
    resp = requests.post(
        PATH + "shorten-anon",
        json={"url": url, "expires_in_hours": expires_in_hours},
    )
    resp.raise_for_status()
    return resp.json()

def get_redirect(short_code: str) -> str:
    """
    Redirects a short code to the original URL.
    """
    resp = requests.get(PATH + short_code, allow_redirects=False)
    return resp.headers["Location"]

def check_health():
    resp = requests.get(PATH + "health")
    return resp.json()

if __name__ == "__main__":
    health = check_health()
    print(f"Health: {health}")

    url = "https://www.google.com"
    expires_in_hours = 24
    
    try:
        resp = shorten_anon(url, expires_in_hours)
        print(f"Short Code: {resp['short_code']}")
        print(f"Short URL: {resp['short_url']}")
        print(f"Original URL: {resp['original_url']}")
        print(f"Expires At: {resp['expires_at']}")
        
        redirect_url = get_redirect(resp['short_code'])
        print(f"Redirect URL: {redirect_url}")
    except requests.exceptions.RequestException as e:
        print(f"HTTP Request failed: {e.response.status_code} {e.response.text}")
    except KeyError as e:
        print(f"Missing expected data in response: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")