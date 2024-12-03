from request_util import try_request
import json
import asyncio
import aiohttp
from aiohttp import ClientSession
from typing import Coroutine, Union, List


async def fetch_page(url: str, session: ClientSession, max_try: int, category_id: int, urlKey: str, page: int) -> Union[Coroutine, None]:
    # Define the parameters for the API request
    params = {
        'limit': '40',  # Number of items per page
        'include': 'advertisement',  # Include advertisements in the response
        'aggregations': '2',  # Aggregation parameter
        'version': 'home-personalized',  # Version of the API
        'category': str(category_id),  # Category ID
        'page': str(page),  # Page number
        'urlKey': urlKey,  # URL key for the product category
    }

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

    # Log failed request parameters
    with open('data/product_fail.jsonl', 'a+') as f:
        f.write(json.dumps(params) + '\n')

    print(f"All attempts failed with {params}")  # Notify if all attempts failed
    return None  # Return None if the request failed

async def repeat_task(page: int, max_try: int) -> List:
    # Create an asynchronous session for making HTTP requests
    async with aiohttp.ClientSession() as session:
        tasks = []  # List to hold tasks for concurrent execution
        url = 'https://tiki.vn/api/personalish/v1/blocks/listings'
        for category in categories:
            category_id = category[1].lstrip('c')  # Get category ID without the leading 'c'
            urlKey = category[0]  # Get the URL key for the category
            
            tasks.append(fetch_page(url, session, max_try=max_try, category_id=category_id, urlKey=urlKey, page=page))
            await asyncio.sleep(0.1)  # Optional delay between task submissions

        results = await asyncio.gather(*tasks)  # Execute all tasks concurrently
        return results

async def main(page, max_try) -> None:
    # Run the repeat_task function to fetch data
    results = await repeat_task(page, max_try)
    # Define the keys to extract from the product data
    product_keys = ['id', 'sku', 'seller_id', 'seller_product_id', 'name', 'brand_name', 'price', 'discount', 'discount_rate', 'original_price', 'quantity_sold', 'availability']
    
    # Process the results to extract product data
    for page in results:
        if page and 'data' in page.keys():  # Ensure 'data' key exists in the response
            with open('data/products.jsonl', 'a+') as f:
                for product in page['data']:
                    product_data = {}
                    for key in product_keys:
                        product_data[key] = product.get(key)
                    f.write(json.dumps(product_data) + '\n')  # Write product data to JSONL

# Run the main function
if __name__ == "__main__":
    # Define headers to mimic a browser request
    headers = {
        'authority': 'tiki.vn',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
        'x-guest-token': 'jfuLxq4eVS1Jvs2bDKp8kMC5UzGT9nhO',
    }

    # Fetch category data from Tiki API
    category_response = try_request("https://api.tiki.vn/raiden/v2/menu-config", params={"platform": "desktop"}, headers=headers)
    # Parse categories from the response
    categories = category_response["menu_block"]["items"]
    # Extract relevant category links and IDs as tuples
    categories = [tuple(category["link"].split("/")[-2:]) for category in categories]

    num_page = 50
    for page in range(1, num_page + 1):
        asyncio.run(main(page, max_try=5))  # Start the asynchronous main function with num_page set to -1
        print(f"-------------finish page :{page}-------------")
