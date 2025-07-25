''' this code is used for testing purposes '''
import requests
import pandas as pd
import json

# API Gateway API_URL
API_URL = "https://0a419bv6d5.execute-api.us-east-1.amazonaws.com/dev"
# API_HEADERS for the request
API_HEADERS = {
    "Content-Type": "application/json"
}
# The body of the request with the report date
API_body = {
    "execution_date": "2025-07-25"
}
# Send the POST request with the correct JSON body
response = requests.post(API_URL,
                         json=API_body,
                         headers=API_HEADERS,
                         timeout=60)
# Output the response status and the content
# print(response.status_code)  # Should print 200

data_list = response.json()

for item in data_list:
    print(item)

# create the dataframe
# df = pd.DataFrame(response.json())
# print(df.head(10))

