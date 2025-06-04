# modules/business_central.py

import requests
import logging

class BusinessCentralClient:
    def __init__(self, url, company_id, access_token, journal_id=None, logger=None):
        self.url = url
        self.company_id = company_id
        self.access_token = access_token
        self.journal_id = journal_id  # Optional if you're using create_payment
        self.logger = logger or logging.getLogger(__name__)

    def get_customer_info(self, customer_name):
        if not customer_name:
            return None

        customer_name = str(customer_name).strip().replace('&', '%26')
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        endpoint = f"{self.url}/companies({self.company_id})/customers?$filter=contains(displayName,'{customer_name}')"

        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            customers = response.json().get('value', [])

            if not customers:
                return None

            customer = customers[0]
            # If first customer is blocked, try the second
            if customer.get('blocked') == 'All' and len(customers) > 1:
                self.logger.info(f"First customer '{customer['displayName']}' is blocked. Using next one.")
                customer = customers[1]

            return {
                'customerId': customer.get('id'),
                'customerNumber': customer.get('number'),
                'customerName': customer.get('displayName')
            }

        except Exception as e:
            self.logger.error(f"Customer lookup failed: {e}")
            return None

    def create_customer_journal_line(self, payload):
        if not self.access_token:
            self.logger.error("Missing access token.")
            return None

        if not self.journal_id:
            self.logger.error("Missing journal_id for payment creation.")
            return None

        endpoint = f"{self.url}/companies({self.company_id})/customerPayments"
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

        try:
            response = requests.post(endpoint, headers=headers, json=payload)
            if response.status_code == 201:
                return response.json().get('id')
            else:
                self.logger.warning(f"API error {response.status_code}: {response.text}")
        except Exception as e:
            self.logger.error(f"Payment creation failed: {e}")
        return None
