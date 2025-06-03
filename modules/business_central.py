import os
import sys

def get_customer_info(self, customer_name):
    if not customer_name:
        return None
    customer_name = str(customer_name).strip().replace('&', '%26')
    if not self.access_token:
        self.access_token = get_access_token()
    headers = {'Authorization': f'Bearer {self.access_token}', 'Content-Type': 'application/json'}
    endpoint = f"{self.url}/companies({self.company_id})/customers?$filter=contains(displayName,'{customer_name}')"
    try:
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            customers = response.json().get('value', [])
            if customers:
                # Check if the first customer is blocked
                if customers[0].get('blocked') == "All" and len(customers) > 1:
                    # If first customer is blocked and there's another customer, use the next one
                    self.logger.info(f"First customer {customers[0].get('displayName')} is blocked, using next customer {customers[1].get('displayName')}")
                    return {
                        'customerId': customers[1].get('id'),
                        'customerNumber': customers[1].get('number'),
                        'customerName': customers[1].get('displayName')
                    }
                else:
                    # Use the first customer as before
                    return {
                        'customerId': customers[0].get('id'),
                        'customerNumber': customers[0].get('number'),
                        'customerName': customers[0].get('displayName')
                    }
    except Exception as e:
        self.logger.error(f"Customer lookup failed: {e}")
    return None

def create_payment(self, customer_info, row):
    if not self.access_token:
        self.access_token = get_access_token()

    try:
        # Handle credit amount with better error handling
        amount_str = str(row['Credit']).strip()
        self.logger.info(f"Processing Credit Amount: '{amount_str}'")
        
        # Skip empty values
        if not amount_str or amount_str == '':
            self.logger.warning("Empty Credit Amount, skipping row")
            self.stats['failed'] += 1
            return None
            
        # Convert to float with better error handling
        try:
            # Remove commas before conversion
            cleaned_amount = amount_str.replace(',', '')
            amount = float(cleaned_amount)
            self.logger.info(f"Successfully converted '{amount_str}' to {amount}")
        except ValueError as e:
            self.logger.warning(f"Failed to convert Credit Amount '{amount_str}' to float: {e}")
            self.stats['failed'] += 1
            return None
            
        amount = -abs(amount)  # Ensure negative for payments
        
        self.logger.info(f"Final amount value: {amount}")
        
    except Exception as e:
        self.logger.warning(f"Invalid amount format: {row.get('Credit', 'N/A')}, error: {e}")
        self.stats['failed'] += 1
        return None

    if not row.get('FormattedDate'):
        # Try to format the date here as a fallback
        try:
            from dateutil import parser
            date_str = str(row.get('Posting date', ''))
            if date_str:
                formatted_date = parser.parse(date_str).strftime('%Y-%m-%d')
                row['FormattedDate'] = formatted_date
            else:
                self.logger.warning("Missing posting date, skipping row")
                self.stats['failed'] += 1
                return None
        except Exception as e:
            self.logger.warning(f"Invalid date format: {row.get('Posting date', 'N/A')}, error: {e}")
            self.stats['failed'] += 1
            return None

    # Get description, with fallback to customer name
    description = ''
    if pd.notna(row.get('DESCRIPTION')) and str(row.get('DESCRIPTION')).strip():
        description = str(row.get('DESCRIPTION')).strip()
    else:
        description = customer_info.get('customerName', '')
        
    if not description:
        description = f"Payment from {customer_info.get('customerNumber', 'unknown')}"


    payload = {
        "journalId": self.journal_id,
        "journalDisplayName": "MBB",
        "customerId": customer_info['customerId'],
        "customerNumber": customer_info['customerNumber'],
        "postingDate": row['FormattedDate'],
        "amount": amount,
        "description": description
    }

    headers = {
        'Authorization': f'Bearer {self.access_token}',
        'Content-Type': 'application/json'
    }

    endpoint = f"{self.url}/companies({self.company_id})/customerPayments"

    try:
        response = requests.post(endpoint, headers=headers, json=payload)
        if response.status_code == 201:
            self.stats['processed'] += 1
            return response.json().get('id')
        else:
            self.logger.warning(f"API error {response.status_code}: {response.text}")
            self.stats['failed'] += 1
    except Exception as e:
        self.logger.error(f"Payment creation failed: {e}")
        self.stats['failed'] += 1
    return None

def process(self):
    df = self.read_csv_file()
    for i, row in df.iterrows():
        if row.get('STATUS') == 'Transferred':
            continue

        name = row.get('CUSTOMER_NAME')
        if pd.isna(name) or not str(name).strip():
            self.logger.info(f"Skipping row {i+2}: Missing customer name")
            self.not_transferred_rows.append(row)
            continue

        customer_info = self.get_customer_info(name)
        if not customer_info:
            self.logger.info(f"Skipping row {i+2}: Customer not found - {name}")
            self.not_transferred_rows.append(row)
            continue

        payment_id = self.create_payment(customer_info, row)
        if payment_id:
            df.at[i, 'STATUS'] = 'Transferred'
            df.at[i, 'payment_ID'] = str(payment_id)

    self.save_updated_csv(df)
    self.save_not_transferred_rows()
    self.logger.info(f"Processed: {self.stats['processed']}, Failed: {self.stats['failed']}")