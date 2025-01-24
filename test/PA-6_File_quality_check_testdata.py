import random
import string
from datetime import datetime, timedelta

# Helper functions
def random_date(start, end):
    """Generate a random date between start and end dates."""
    return start + timedelta(days=random.randint(0, (end - start).days))

def random_string(length=5):
    """Generate a random string of fixed length."""
    return ''.join(random.choices(string.ascii_uppercase, k=length))

def random_numeric_string(length=5):
    """Generate a random numeric string of fixed length."""
    return ''.join(random.choices(string.digits, k=length))

# Test Data Categories

# Happy path test data (valid, expected scenarios)
happy_path_data = [
    # Valid records with all fields correctly filled
    {"country_cd": "US", "qty_sold": 100, "product_id": "P12345", "Date": "2023-10-01"},
    {"country_cd": "CA", "qty_sold": 200, "product_id": "P12346", "Date": "2023-10-02"},
    {"country_cd": "GB", "qty_sold": 150, "product_id": "P12347", "Date": "2023-10-03"},
]

# Edge case test data (boundary conditions)
edge_case_data = [
    # Minimum and maximum values for qty_sold
    {"country_cd": "AU", "qty_sold": 0, "product_id": "P12348", "Date": "2023-10-04"},
    {"country_cd": "IN", "qty_sold": 999999, "product_id": "P12349", "Date": "2023-10-05"},
    # Date at the boundary of the year
    {"country_cd": "FR", "qty_sold": 300, "product_id": "P12350", "Date": "2023-12-31"},
]

# Error case test data (invalid inputs)
error_case_data = [
    # Missing country_cd
    {"country_cd": None, "qty_sold": 100, "product_id": "P12351", "Date": "2023-10-06"},
    # Non-numeric qty_sold
    {"country_cd": "DE", "qty_sold": "abc", "product_id": "P12352", "Date": "2023-10-07"},
    # Duplicate product_id
    {"country_cd": "JP", "qty_sold": 400, "product_id": "P12345", "Date": "2023-10-08"},
    # Invalid date format
    {"country_cd": "BR", "qty_sold": 500, "product_id": "P12353", "Date": "10-09-2023"},
]

# Special character and format test data
special_character_data = [
    # Special characters in country_cd
    {"country_cd": "U$@", "qty_sold": 600, "product_id": "P12354", "Date": "2023-10-10"},
    # Special characters in product_id
    {"country_cd": "IT", "qty_sold": 700, "product_id": "P@#123", "Date": "2023-10-11"},
    # Date with special characters
    {"country_cd": "ES", "qty_sold": 800, "product_id": "P12355", "Date": "2023/10/12"},
]

# Combine all test data
test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Print test data
for record in test_data:
    print(record)


This code generates a set of test data records for a file quality check process, covering various scenarios such as valid inputs, edge cases, error cases, and special character handling. Each record is structured as a dictionary with fields `country_cd`, `qty_sold`, `product_id`, and `Date`, and the data is printed out for review.