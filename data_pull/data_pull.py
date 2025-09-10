# Import the necessary libraries
import blpapi
import pandas as pd

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
                    date = row.getElementAsDatetime("date").date()
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

def main():
    print("Data pull module is ready to use.")
    # You can add code here to call get_bdh_data with specific parameters if needed.
    
# --- Example Usage ---
if __name__ == '__main__':
    main()
    # print("Starting data pull...")
    # tickers_to_load = ["AAPL US Equity", "MSFT US Equity"]
    # fields_to_load = ["PX_LAST", "PX_VOLUME"]
    # start = "20240101"
    # end = "20241231"
    
    # historical_data = get_bdh_data(tickers_to_load, fields_to_load, start, end)
    
    # if historical_data is not None:
    #     print("Successfully downloaded data:")
    #     print(historical_data.head())