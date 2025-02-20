import pandas as pd
import logging
import numpy as np
from datetime import datetime
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

# File paths
RAW_DATA_PATH = "data/raw_data.json"  # Replace with actual path to your raw data
PROCESSED_DATA_PATH = "data/processed_data.csv"  # Replace with the path where you want to store processed data

# Function to load raw data
def load_data(file_path):
    """Load the raw data from the specified file path."""
    if os.path.exists(file_path):
        logger.info(f"Loading data from {file_path}...")
        try:
            # Assuming JSON format, adjust for your data format
            data = pd.read_json(file_path)
            logger.info("Data loaded successfully.")
            return data
        except ValueError as e:
            logger.error(f"Error loading data: {e}")
            return None
    else:
        logger.error(f"File not found: {file_path}")
        return None

# Function to clean data (e.g., remove duplicates, handle missing values)
def clean_data(data):
    """Clean the data by removing duplicates and handling missing values."""
    if data is None:
        logger.error("No data to clean.")
        return None

    logger.info("Cleaning data...")

    # Remove duplicates
    data = data.drop_duplicates()
    logger.info(f"Removed {data.duplicated().sum()} duplicate rows.")

    # Handle missing values (example: filling with a placeholder or mean)
    data = data.fillna(method='ffill')  # Forward fill missing values
    logger.info(f"Filled missing values.")

    return data

# Function to transform data (e.g., adding new columns or converting data types)
def transform_data(data):
    """Transform the data by adding new columns or converting data types."""
    if data is None:
        logger.error("No data to transform.")
        return None

    logger.info("Transforming data...")

    # Example: Converting a date column to datetime format (if exists)
    if 'date' in data.columns:
        data['date'] = pd.to_datetime(data['date'], errors='coerce')
        logger.info("Converted 'date' column to datetime.")

    # Example: Adding a new column based on existing data
    data['processed_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("Added 'processed_date' column with current timestamp.")

    return data

# Function to perform data analysis (e.g., aggregation, filtering)
def analyze_data(data):
    """Perform analysis on the data (e.g., filtering, aggregation)."""
    if data is None:
        logger.error("No data to analyze.")
        return None

    logger.info("Analyzing data...")

    # Example: Filter data for a specific condition (e.g., posts by a certain user)
    filtered_data = data[data['userId'] == 1]  # Adjust according to your data structure
    logger.info(f"Filtered data to include {len(filtered_data)} rows where userId is 1.")

    # Example: Aggregation (e.g., count the number of posts per user)
    user_posts_count = data.groupby('userId').size()
    logger.info(f"Post counts per user: {user_posts_count.head()}")  # Show top 5 users

    return filtered_data, user_posts_count

# Function to save processed data
def save_data(data, output_path):
    """Save the processed data to a file."""
    if data is None:
        logger.error("No data to save.")
        return

    logger.info(f"Saving processed data to {output_path}...")
    try:
        data.to_csv(output_path, index=False)
        logger.info(f"Data saved successfully to {output_path}")
    except Exception as e:
        logger.error(f"Error saving data: {e}")

# Main function to execute the data processing pipeline
def main():
    logger.info("Starting data processing pipeline...")

    # Step 1: Load raw data
    raw_data = load_data(RAW_DATA_PATH)

    # Step 2: Clean data
    cleaned_data = clean_data(raw_data)

    # Step 3: Transform data
    transformed_data = transform_data(cleaned_data)

    # Step 4: Analyze data
    filtered_data, user_posts_count = analyze_data(transformed_data)

    # Step 5: Save processed data
    save_data(filtered_data, PROCESSED_DATA_PATH)

    # Optionally, you can save the analysis results to a separate file
    user_posts_count.to_csv("data/user_posts_count.csv", header=True)

    logger.info("Data processing pipeline completed.")

# Run the script every day at a specific time (for example, 10 AM)
if __name__ == "__main__":
    main()
