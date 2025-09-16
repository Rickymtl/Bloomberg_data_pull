# Import the necessary libraries
import blpapi
import os
import sys
import pandas as pd
from datetime import date, timedelta
import datetime
from itertools import product

# Define constants for the API session
SESSION_STARTED = blpapi.Name("SessionStarted")
SESSION_STARTUP_FAILURE = blpapi.Name("SessionStartupFailure")
SERVICE_OPENED = blpapi.Name("ServiceOpened")
SERVICE_OPEN_FAILURE = blpapi.Name("ServiceOpenFailure")
ERROR_INFO = blpapi.Name("ErrorInfo")
SECURITY_DATA = blpapi.Name("securityData")

# --- Main Function to Get Bulk Historical Data ---
def get_bdh_data(tickers, fields, start_date, end_date):
    """
    Downloads historical data (BDH) from the Bloomberg API.
    """
    # Boilerplate connection logic
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost('localhost')
    sessionOptions.setServerPort(8194)
    session = blpapi.Session(sessionOptions)

    if not session.start():
        print("Failed to start session.")
        return

    if not session.openService("//blp/refdata"):
        print("Failed to open //blp/refdata")
        return

    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("HistoricalDataRequest")

    # Add tickers and fields to the request
    for ticker in tickers:
        request.getElement("securities").appendValue(ticker)
    for field in fields:
        request.getElement("fields").appendValue(field)

    # Set request parameters
    request.set("periodicitySelection", "DAILY")
    request.set("startDate", start_date)
    request.set("endDate", end_date)

    session.sendRequest(request)
    
    all_data = []
    
    # Process the response events
    while(True):
        ev = session.nextEvent(500)
        for msg in ev:
            if msg.hasElement(SECURITY_DATA):
                sec_data = msg.getElement(SECURITY_DATA)
                ticker = sec_data.getElementAsString("security")
                field_data = sec_data.getElement("fieldData")
                for i in range(field_data.numValues()):
                    row = field_data.getValueAsElement(i)
                    date = row.getElementAsDatetime("date")
                    for field in fields:
                        if row.hasElement(field):
                            value = row.getElementAsFloat(field)
                            all_data.append([date, ticker, field, value])
        if ev.eventType() == blpapi.Event.RESPONSE:
            break
            
    session.stop()
    
    # Convert the received data to a pandas DataFrame
    df = pd.DataFrame(all_data, columns=['date', 'ticker', 'field', 'value'])
    # Pivot the table to a more usable format
    pivot_df = df.pivot_table(index='date', columns=['ticker', 'field'], values='value')
    return pivot_df

def generate_tickers():
    # result = []
    # start_date = date(2022,1,1)
    # end_date = datetime.date.today()
    # delta = timedelta(days=1)
    # while start_date < end_date:
    #     result.append(start_date.strftime("%m/%d/%y"))
    #     start_date += delta
    # options = ["C", "P"]
    # strikes = [5*i for i in range(80,161)]
    # all_combos = list(product(result, options, strikes))
    # all_tickers = [f"SPY US {i[0]} {i[1]}{i[2]} Equity" for i in all_combos]
    # print(all_tickers[:5])
    # print(len(all_tickers))
    all_tickers = []
    df = pd.read_csv("tickers.csv")
    for index, row in df.iterrows():
        all_tickers.append(row['Security'])
    # print(all_tickers)
    return all_tickers


def main(tickers, fields):
    print("Starting data pull...")
    # all_tickers = generate_tickers()
    # for tickers_to_load in all_tickers:
    # tickers_to_load = ["SPY US 10/17/25 P625 Equity"]
    # tickers_to_load = all_tickers
    # fields_to_load = ["OPT_IMPLIED_VOLATILITY_MID"]
    # fields_to_load = ["IVOL_MID"]

    tickers_to_load = [tickers]
    fields_to_load = [fields]

    start = "20220101"
    end = "20250912"
    # mn
    historical_data = get_bdh_data(tickers_to_load, fields_to_load, start, end)
    #
    if historical_data is not None:
        print("Successfully downloaded data:")
        print(tickers_to_load)
        print(historical_data.head())

        if not historical_data.empty:
            os.makedirs("out", exist_ok=True)
            ticker = tickers_to_load[0].replace(" /", "")
            historical_data.to_csv(f"out/{ticker}.csv")
        # You can add code here to call get_bdh_data with specific parameters if needed.
    
# --- Example Usage ---
if __name__ == '__main__':
    if len(sys.argv) < 3:
        main("VIX Index", "PX_LAST")
    else:
        main(sys.argv[1], sys.argv[2])
    # generate_tickers()