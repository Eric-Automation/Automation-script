import requests
import logging
import time
import os
from requests.exceptions import RequestException, Timeout, TooManyRedirects
from time import sleep

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

# API URL (you can replace this with your own API endpoint)
API_URL = "https://jsonplaceholder.typicode.com/posts"

# Get the API Key from environment variables (for security)
API_KEY = os.getenv("API_KEY", "your_api_key_here")  # Ensure your key is stored securely

# Headers for the request
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}"  # Replace with actual API Key if required
}

# Optional: Set retry parameters
MAX_RETRIES = 5
RETRY_DELAY = 5  # Delay between retries in seconds
EXTRA_DELAY_ON_FAILURE = 3  # Extra delay after each retry failure (Exponential backoff)

# Function to handle rate limiting (if applicable)
def handle_rate_limiting(response):
    """Check and handle rate limiting headers."""
    if response.status_code == 429:  # Rate limit exceeded
        reset_time = int(response.headers.get("X-RateLimit-Reset", time.time() + 60))  # Example header
        wait_time = reset_time - time.time()
        logger.warning(f"Rate limit exceeded, retrying in {wait_time} seconds.")
        sleep(wait_time)
        return True
    return False

# Function to make the API request with retries
def make_api_request():
    """Make the GET request to the API with retries and error handling."""
    retries = 0
    while retries < MAX_RETRIES:
        try:
            logger.info(f"Sending request to API (Attempt {retries + 1}/{MAX_RETRIES})...")
            response = requests.get(API_URL, headers=headers, timeout=10)  # Timeout for the request
            
            # Check for rate limiting
            if handle_rate_limiting(response):
                continue  # If rate-limited, retry after waiting

            # Check if the response is successful
            if response.status_code == 200:
                logger.info("API request successful!")
                return response.json()  # Parse the JSON data from the response
            else:
                logger.error(f"Failed to retrieve data. Status code: {response.status_code}")
                return None

        except Timeout:
            logger.error("Request timed out. Retrying...")
        except TooManyRedirects:
            logger.error("Too many redirects. Check the URL.")
        except RequestException as e:
            logger.error(f"An error occurred: {e}")
        
        retries += 1
        logger.info(f"Retrying in {RETRY_DELAY * (2 ** retries)} seconds...")
        sleep(RETRY_DELAY * (2 ** retries))  # Exponential backoff delay between retries
    
    logger.error("Max retries reached. Request failed.")
    return None

# Function to process the API data
def process_api_data(data):
    """Process the data received from the API."""
    if not data:
        logger.error("No data to process.")
        return

    # Example: Print the titles of the posts (customize based on your API structure)
    logger.info("Processing API data...")
    for item in data:
        title = item.get('title', 'No Title')
        logger.info(f"Post Title: {title}")

# Function to handle pagination (if applicable)
def fetch_all_data():
    """Fetch all pages of data (if API supports pagination)."""
    all_data = []
    page = 1
    while True:
        logger.info(f"Fetching page {page}...")
        response = requests.get(API_URL, params={"page": page}, headers=headers)
        
        # Check for rate limiting and handle it
        if handle_rate_limiting(response):
            continue
        
        if response.status_code == 200:
            data = response.json()
            if not data:
                break  # No more data to fetch
            all_data.extend(data)
            page += 1
        else:
            logger.error(f"Failed to retrieve page {page}. Status code: {response.status_code}")
            break

    return all_data

# Function to send data via POST request
def send_post_request(data):
    """Send data to the API using POST."""
    try:
        logger.info("Sending POST request...")
        response = requests.post(API_URL, json=data, headers=headers)

        if response.status_code == 201:
            logger.info("POST request successful!")
            return response.json()
        else:
            logger.error(f"Failed to post data. Status code: {response.status_code}")
            return None
    except RequestException as e:
        logger.error(f"An error occurred during POST request: {e}")
        return None

# Main function to execute the script
def main():
    logger.info("Starting API integration script...")
    
    # Fetch data (handle pagination if needed)
    data = fetch_all_data()
    
    # Process the received data
    if data:
        process_api_data(data)
    
    # Example: Send a POST request
    new_post_data = {
        "title": "foo",
        "body": "bar",
        "userId": 1
    }
    send_post_request(new_post_data)
    
    logger.info("Script execution completed.")

# Run the script every 10 minutes (customizable)
if __name__ == "__main__":
    while True
        main()
        logger.info("Sleeping for 10 minutes before the next request...")
        sleep(600)  # Wait for 10 minutes before running the script again
