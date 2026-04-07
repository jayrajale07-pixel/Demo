import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
import sqlite3
import uuid

# --- CONFIGURATION ---
BSE_SCRIPS_CSV = 'C:\\Users\\JANHVI\\Desktop\\test\\agent\\Equity.csv'
CHROMEDRIVER_PATH = 'C:\\Users\\JANHVI\\Desktop\\test\\agent\\chromedriver-win64\\chromedriver-win64\\chromedriver.exe'
OUTPUT_DB = 'indian_stock_data.db'

# Set up Chrome options
chrome_options = Options()
# chrome_options.add_argument("--headless")  # Uncomment for headless mode
service = Service(CHROMEDRIVER_PATH)
driver = webdriver.Chrome(service=service, options=chrome_options)

def get_bse_tickers():
    """Read scrip codes from the Equity.csv file."""
    try:
        df = pd.read_csv(BSE_SCRIPS_CSV)
        return df['Security Code'].astype(str).tolist()
    except Exception as e:
        print(f"Error reading tickers from file: {e}")
        return []

def fetch_historical_data(scrip_code, start_date, end_date):
    """Fetch historical data for a given scrip code from BSE India."""
    url = f"https://www.bseindia.com/stock-share-price/any-name/any-symbol/{scrip_code}/"
    driver.get(url)
    time.sleep(2)  # Wait for initial page load

    data = []
    try:
        # Navigate to Historical Data tab (adjust selector as needed)
        historical_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//a[contains(text(), "Historical Data")]'))
        )
        historical_tab.click()
        time.sleep(2)

        # Set date range (example: input fields for date)
        start_date_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "fromDate"))
        )
        end_date_input = driver.find_element(By.ID, "toDate")
        start_date_input.clear()
        end_date_input.clear()
        start_date_input.send_keys(start_date.strftime("%d/%m/%Y"))
        end_date_input.send_keys(end_date.strftime("%d/%m/%Y"))

        # Submit the date range form
        submit_button = driver.find_element(By.XPATH, '//button[contains(text(), "Get Data")]')
        submit_button.click()
        time.sleep(3)

        # Extract data from the table
        table = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//table[@id="historicalDataTable"]'))
        )
        rows = table.find_elements(By.TAG_NAME, "tr")
        headers = [header.text.strip() for header in rows[0].find_elements(By.TAG_NAME, "th")]
        
        for row in rows[1:]:  # Skip header row
            cols = row.find_elements(By.TAG_NAME, "td")
            if cols:
                row_data = {headers[i]: cols[i].text.strip() for i in range(len(cols))}
                row_data['Scrip Code'] = scrip_code
                # Calculate average price
                if 'High' in row_data and 'Low' in row_data and 'Close' in row_data:
                    try:
                        high = float(row_data['High'])
                        low = float(row_data['Low'])
                        close = float(row_data['Close'])
                        row_data['Average'] = round((high + low + close) / 3, 2)
                    except ValueError:
                        row_data['Average'] = None
                data.append(row_data)
    except Exception as e:
        print(f"Error fetching data for {scrip_code}: {e}")
    return data

def save_to_database(data, db_name=OUTPUT_DB):
    """Save data to SQLite database."""
    conn = sqlite3.connect(db_name)
    for record in data:
        df = pd.DataFrame([record])
        df.to_sql('stock_data', conn, if_exists='append', index=False)
    conn.close()

def main():
    # Define date range (e.g., last 1 year)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    # Get list of scrip codes
    codes = get_bse_tickers()[:10]  # Limit for testing
    all_data = []
    
    for code in codes:
        print(f"Fetching historical data for {code}...")
        historical_data = fetch_historical_data(code, start_date, end_date)
        all_data.extend(historical_data)
        time.sleep(1)  # Avoid overwhelming the server
    
    # Save to database
    if all_data:
        save_to_database(all_data)
        print(f"Saved data to {OUTPUT_DB}")
    
    # Optionally save to CSV
    df = pd.DataFrame(all_data)
    df.to_csv("bse_historical_data.csv", index=False)
    print("Saved to bse_historical_data.csv")
    
    driver.quit()

if __name__ == "__main__":
    # Create SQLite table if it doesn't exist
    conn = sqlite3.connect(OUTPUT_DB)
    conn.execute('''CREATE TABLE IF NOT EXISTS stock_data
                    (Scrip_Code TEXT, Date TEXT, Open REAL, High REAL, Low REAL, 
                     Close REAL, Volume INTEGER, Average REAL)''')
    conn.close()
    main()