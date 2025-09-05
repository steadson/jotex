# modules/business_central.py

import requests
import logging
import json

class BusinessCentralClient:
    def __init__(self, url, company_id, access_token, journal_id=None, logger=None):
        self.url = url
        self.company_id = company_id
        self.access_token = access_token
        self.journal_id = journal_id  # Optional if you're using create_payment
        self.logger = logger or logging.getLogger(__name__)
        self.api_endpoint_used = 'customers'  # Track which API endpoint was used

        # Log initialization
        self.logger.info(f"BusinessCentralClient initialized:")
        self.logger.info(f"  URL: {url}")
        self.logger.info(f"  Company ID: {company_id}")
        self.logger.info(f"  Journal ID: {journal_id}")
        self.logger.info(f"  Access Token: {access_token}")


    def get_customer_info(self, customer_name):
        """Get customer information with detailed API logging."""
        if not customer_name:
            return None
        customer_name = str(customer_name).strip() 
        customer_name = customer_name.replace('%26amp;', '&')
        customer_name = customer_name.replace('%26', '&')
        # Then encode for API call
        customer_name = customer_name.replace('&', '%26')

        # customer_name = str(customer_name).strip().replace('&', '%26')
        self.logger.debug(f"API: Looking up customer '{customer_name}'")
        
        try:
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }
            endpoint = f"{self.url}/companies({self.company_id})/customers?$filter=contains(displayName,'{customer_name}')"
            
            self.logger.debug(f"API: GET {endpoint}")
            
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            
            self.logger.debug(f"API: Response status - {response.status_code}")
            
            customers = response.json().get('value', [])
            self.logger.debug(f"API: Found {len(customers)} customer(s)")

            if not customers:
                self.logger.warning(f"API: No customer found for '{customer_name}'")
                return None

            customer = customers[0]
            # If first customer is blocked, try the second
            if customer.get('blocked') == 'All' and len(customers) > 1:
                self.logger.warning(f"API: First customer '{customer['displayName']}' is blocked. Using next one.")
                customer = customers[1]
            elif customer.get('blocked') == 'All':
                self.logger.warning(f"API: Customer {customer.get('number')} is blocked")

            customer_info = {
                'customerId': customer.get('id'),
                'customerNumber': customer.get('number'),
                'customerName': customer.get('displayName'),
                'blocked': customer.get('blocked') == 'All'
            }
            
            self.logger.debug(f"API: Customer details - Number: {customer_info['customerNumber']}, Name: {customer_info['customerName']}, Blocked: {customer_info['blocked']}")
            
            return customer_info

        except Exception as e:
            self.logger.error(f"API: Exception during customer lookup for '{customer_name}': {e}")
            return None

    def get_all_customers(self):
        """Fetch all customers from Business Central with expanded contact information."""
        self.logger.info("API: Fetching all customers from Business Central")
        
        try:
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }
            
            all_customers = []
            skip = 0
            top = 1000
            
            while True:
                # Try different approaches to get contact information
                # Option 1: Try with contacts endpoint first
                endpoint = f"{self.url}/companies({self.company_id})/contacts?$top={top}&$skip={skip}"
                self.logger.debug(f"API: GET {endpoint} (with contacts expansion)")
                
                response = requests.get(endpoint, headers=headers)
                if response.status_code == 200:
                    self.api_endpoint_used = 'contacts'

                # If contacts endpoint doesn't work, fall back to customers endpoint
                if response.status_code != 200:
                    self.logger.warning("API: Contacts endpoint failed, trying customers endpoint")
                    endpoint = f"{self.url}/companies({self.company_id})/customers?$top={top}&$skip={skip}"
                    self.logger.debug(f"API: GET {endpoint}")
                    response = requests.get(endpoint, headers=headers)
                    self.api_endpoint_used = 'customers'

                
                response.raise_for_status()
                data = response.json()
                customers = data.get('value', [])
                
                if not customers:
                    break
                
                # Log the first customer's complete structure for debugging
                if len(all_customers) == 0 and customers:
                    self.logger.info(f"API: First customer available fields: {list(customers[0].keys())}")
                    self.logger.info(f"API: First customer complete data: {customers[0]}")
                
                all_customers.extend(customers)
                skip += top
                
                self.logger.debug(f"API: Fetched {len(customers)} customers (total: {len(all_customers)})")
                
                if len(customers) < top:
                    break
            
            self.logger.info(f"API: Successfully fetched {len(all_customers)} total customers using {self.api_endpoint_used} endpoint")
            return all_customers
            
        except Exception as e:
            self.logger.error(f"API: Exception during customer fetch: {e}")
            return []

    def get_customer_contacts_for_individual(self, customer_id):
        """Get contacts for a specific customer using the contactsInformation navigation property."""
        try:
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }
            
            # Use the contactsInformation navigation property
            endpoint = f"{self.url}/companies({self.company_id})/customers({customer_id})/contactsInformation"
            self.logger.debug(f"API: GET {endpoint}")
            
            response = requests.get(endpoint, headers=headers)
            if response.status_code == 200:
                data = response.json()
                contacts = data.get('value', [])
                if contacts:
                    # Return the first contact's name
                    return contacts[0].get('contactName', '')
            else:
                self.logger.debug(f"API: contactsInformation returned {response.status_code}")
            
            return ''
            
        except Exception as e:
            self.logger.debug(f"API: Could not fetch contactsInformation for customer {customer_id}: {e}")
            return ''
    
    def export_customers_with_contact_to_csv(self, output_file):
        """Export all customers with contact information to CSV file for BC cache."""
        import pandas as pd
        
        customers = self.get_all_customers()
        if not customers:
            self.logger.error("No customers found to export")
            return False
        
        try:
            # Create DataFrame with customer data optimized for BC cache
            customer_data = []
            for customer in customers:
                # For contacts endpoint
                if self.api_endpoint_used == 'contacts':
                    customer_data.append({
                        'CUSTOMER_NAME': customer.get('companyName', ''),
                        'CONTACT': customer.get('displayName', ''),
                        'CUSTOMER_NUMBER': customer.get('number', ''),
                        'CUSTOMER_ID': customer.get('id', '')
                    })
                # For customers endpoint
                else:
                    customer_data.append({
                        'CUSTOMER_NAME': customer.get('displayName', ''),
                        'CONTACT': customer.get('contactName', ''),
                        'CUSTOMER_NUMBER': customer.get('number', ''),
                        'CUSTOMER_ID': customer.get('id', '')
                    })
            
            df = pd.DataFrame(customer_data)
            df.to_csv(output_file, index=False)
            
            self.logger.info(f"Successfully exported {len(customer_data)} customers with contact info to {output_file}")
            return True
        except Exception as e:
            self.logger.error(f"Error exporting customers with contact to CSV: {e}")
            return False
    

    def create_customer_journal_line(self, payload):
        """Create customer journal line with detailed API logging."""
        if not self.access_token:
            self.logger.error("Missing access token.")
            return None

        if not self.journal_id:
            self.logger.error("Missing journal_id for payment creation.")
            return None

        customer_name = payload.get('accountId', 'Unknown')
        amount = payload.get('amount', 0)
        
        self.logger.debug(f"API: Creating payment for {customer_name}, amount: {amount}")
        
        try:
            # Use the working endpoint from the old version
            endpoint = f"{self.url}/companies({self.company_id})/customerPayments"
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }
            
            self.logger.debug(f"API: POST {endpoint}")
            self.logger.debug(f"API: Payload - {json.dumps(payload, indent=2)}")
            
            response = requests.post(endpoint, headers=headers, json=payload)
            
            self.logger.debug(f"API: Response status - {response.status_code}")
            
            if response.status_code == 201:
                data = response.json()
                payment_id = data.get('id')
                self.logger.info(f"API: Payment created successfully - ID: {payment_id} for {customer_name}")
                return payment_id
            else:
                error_msg = response.text
                self.logger.error(f"API: Payment creation failed - Status: {response.status_code}")
                self.logger.error(f"API: Error response: {error_msg}")
                
                # Parse specific error messages
                if "blocked for privacy" in error_msg.lower():
                    self.logger.warning(f"API: Customer {customer_name} is blocked for privacy")
                elif "400" in str(response.status_code):
                    self.logger.warning(f"API: Bad request for {customer_name} - {error_msg}")
                
                return None
                
        except Exception as e:
            self.logger.error(f"API: Exception during payment creation for {customer_name}: {e}")
            return None