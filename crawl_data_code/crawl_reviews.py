import json
import asyncio
import aiohttp
from aiohttp import ClientSession
from typing import Coroutine, Union, List
from math import ceil

# Asynchronous function to fetch reviews from the API
async def fetch_page(url: str, session: ClientSession, max_try: int, seller_product_id: int, id: int, seller_id: int, page: int) -> Union[Coroutine, None]:
    # Parameters for the API request
    params = {
        'page': str(page),
        'spid': str(seller_product_id),
        'product_id': str(id),
        'seller_id': str(seller_id)
    }
    
    # Attempt to fetch data with retries
    for attempt in range(max_try):
        try:
            async with session.get(url, params=params, headers=headers) as response:
                # Check if the request was successful
                if response.status == 200:
                    return await response.json()  # Return the JSON response
                else:
                    print(f"Attempt {attempt + 1}: Received status code {response.status}")
        except aiohttp.ClientError as e:
            print(f"Attempt {attempt + 1}: Request failed with exception: {e}")
        
        await asyncio.sleep(3)  # Wait before retrying

    # Log failed request parameters for troubleshooting
    with open('data/review_fail.jsonl', 'a+') as f:
        f.write(json.dumps(params) + '\n')

    print(f"All attempts failed with {params}")  # Notify if all attempts failed
    return None  # Return None if the request failed

# Asynchronous function to repeat the fetch task for all products
async def repeat_task(num_page: int, max_try: int) -> List:
    # Create an asynchronous session for making HTTP requests
    async with aiohttp.ClientSession() as session:
        tasks = []  # List to hold tasks for concurrent execution
        url = 'https://tiki.vn/api/v2/reviews?limit=5&include=comments,contribute_info,attribute_vote_summary&sort=score%7Cdesc,id%7Cdesc,stars%7Call'

        # Iterate over product data and create fetch tasks
        for product in batch_product:
            seller_product_id = product["seller_product_id"]
            id = product["id"]
            seller_id = product["seller_id"]

            for page in range(1, num_page + 1):
                # Append each fetch task to the tasks list
                tasks.append(fetch_page(url, session, max_try=max_try, seller_product_id=seller_product_id,
                                        id=id, seller_id=seller_id, page=page))
                # await asyncio.sleep(0.1)  # Optional delay between task submissions

        # Execute all tasks concurrently and gather results
        results = await asyncio.gather(*tasks)  
        return results
    
# Main asynchronous function to orchestrate the fetching of reviews
async def main(num_page, max_try) -> None:
    results = await repeat_task(num_page, max_try)  # Fetch reviews for the current page
    review_keys = ["id", "product_id", "content", "rating"]  # Keys to extract from reviews
    for page in results:
        if page and 'data' in page.keys():  # Check if response contains data
            with open('data/reviews.jsonl', 'a+') as f:
                for review in page['data']:
                    review_data = {}
                    for key in review_keys:
                        review_data[key] = review.get(key)  # Extract relevant fields
                    f.write(json.dumps(review_data) + '\n')  # Write to the JSONL file

# Entry point of the script
if __name__ == "__main__":
    # Headers for the HTTP request, mimicking a browser's request
    headers = {
        'authority': 'tiki.vn',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
        'x-guest-token': 'jfuLxq4eVS1Jvs2bDKp8kMC5UzGT9nhO',
    }

    # Columns to extract from the product data
    columns = ['seller_product_id', 'id', 'seller_id']
    product_data = []  # List to store product data

    # Load product data from a JSONL file
    with open("data/products_main.jsonl", "r") as f:
        for line in f:
            product = json.loads(line)  # Parse each line as JSON
            data = {}
            for key in columns:
                data[key] = product[key]  # Extract relevant fields
            product_data.append(data)  # Append extracted data to product_data list

    num_batch = 20

    for i in range(ceil(len(product_data)/num_batch)):
        batch_product = product_data[i * num_batch: (i + 1) * num_batch]
        asyncio.run(main(num_page=1, max_try=5))  # Run the main async function
        print(f"-------------finish batch :{i + 1}-------------")  # Notify when a page is finished
