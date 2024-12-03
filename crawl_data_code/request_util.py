import requests
import time

def try_request(url, params, headers, max_try=5):
    for attempt in range(max_try):
        try:
            response = requests.get(url, headers=headers, params=params)
            # Check if the request was successful
            if response.status_code == 200:
                return response.json()  # Return the parsed JSON response
            else:
                print(f"Attempt {attempt + 1}: Received status code {response.status_code}")
        except requests.RequestException as e:
            print(f"Attempt {attempt + 1}: Request failed with exception: {e}")
            time.sleep(3)
    
    print("All attempts failed.")  # Notify if all attempts failed