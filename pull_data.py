import re
import requests
import urllib.parse
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
import singlestoredb as s2


engine = create_engine(
    url="mysql://admin:IfIpf5AekWOCDLzAFlKbtTU0UGlQAl1O@svc-705e3c0c-7c92-4617-9ba3-ba22787f80ae-dml.aws-oregon-3.svc.singlestore.com:3306/data_ingestion",
)
conn = engine.connect()

def tear_down_url(url):
    # Parse the URL
    parsed_url = urllib.parse.urlparse(url)
    # Extract the query parameters and convert to dictionary
    query_params = urllib.parse.parse_qs(parsed_url.query)
    # Convert single-value lists to values
    urlParams = {k: v[0] if len(v) == 1 else v for k, v in query_params.items()}
    # Add the base URL
    urlParams["url"] = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
    # Return the parameters
    return urlParams


def rebuild_url(urlParams):
    # Separate cayenneExp parameter to avoid double encoding
    cayenne_exp = urlParams.pop("cayenneExp", None)
    # Reconstruct the URL from the dictionary
    base_url = urlParams.pop("url")
    query_string = urllib.parse.urlencode(urlParams, doseq=True)
    # Manually add cayenneExp back to the URL
    if cayenne_exp:
        query_string += f"&cayenneExp={cayenne_exp}"
    return f"{base_url}?{query_string}"


def alter_url(url, start, limit):
    urlParams = tear_down_url(url)
    urlParams["limit"] = limit
    urlParams["start"] = start
    return rebuild_url(urlParams)


def get_last_day_of_month(date):
    """
    Get the last day of the month for a given date.
    """
    next_month = date + relativedelta(months=1, day=1)
    return next_month - relativedelta(days=1)


def increment_dates_in_string(
    base_str, start_date, increment_type, increment_value, iterations
):
    """
    Replace dates in the base string with a start date and increment by a specified type and value over iterations.

    :param base_str: The base string containing the date to be replaced.
    :param start_date: The start date for the replacement in 'YYYY-MM-DD' format.
    :param increment_type: The type of increment ('days', 'months', 'years').
    :param increment_value: The value to increment by.
    :param iterations: The number of iterations to perform.
    :return: A list of modified strings with incremented dates.
    """
    date_strings = []
    current_start_date = datetime.strptime(start_date, "%Y-%m-%d")

    for _ in range(iterations):
        # Calculate new end date based on the increment type
        if increment_type == "days":
            new_end_date = current_start_date + relativedelta(days=increment_value)
        elif increment_type == "months":
            new_end_date = get_last_day_of_month(current_start_date)
        elif increment_type == "years":
            new_end_date = current_start_date + relativedelta(years=increment_value)
            new_end_date = datetime(
                new_end_date.year, 12, 31
            )  # Ensure end date is the last day of the year

        # Format the new start and end dates
        new_start_date_str = current_start_date.strftime("%Y-%m-%d")
        new_end_date_str = new_end_date.strftime("%Y-%m-%d 23:59:59")

        # Manually construct the new string with updated dates
        new_str = f'gameDate<="{new_end_date_str}" and gameDate>="{new_start_date_str}" and gameTypeId=2'

        # Append the new string to the list
        date_strings.append(new_str)

        # Increment the current start date for the next iteration
        current_start_date += relativedelta(**{increment_type: increment_value})

    return date_strings


def iterate_through_time_period(full_url,name):
    start, size, limit = 0, 100, 100
    while size == limit:
        full_url = alter_url(full_url, start, limit)
        response = requests.get(full_url)
        if response.status_code == 200:  # Check if the request was successful
            data = response.json()  # Parse the JSON response
            size = len(data["data"])
            if size == 0:
                print('Skipping Iteration Size 0')
                break
            try:
                print(data["data"][0]["playerId"], size)
            except:
                print(data["data"][0]["gameId"], size)
            df = pd.DataFrame(data["data"])
            df.to_sql(f"player_{name}", con=engine, if_exists="append", index=False)
            start += size
        else:
            print(f"Failed to retrieve data: {response.status_code}")
            break


# The given URL
url_dic = {
    "summary": "https://api.nhle.com/stats/rest/en/skater/summary?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22gameDate%22,%22direction%22:%22ASC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=100&cayenneExp=gameDate%3C=%222000-10-31%2023%3A59%3A59%22%20and%20gameDate%3E=%222000-10-01%22%20and%20gameTypeId=2",
    "bios": "https://api.nhle.com/stats/rest/en/skater/bios?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22lastName%22,%22direction%22:%22ASC_CI%22%7D,%7B%22property%22:%22skaterFullName%22,%22direction%22:%22ASC_CI%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=100&cayenneExp=gameDate%3C=%222000-10-31%2023%3A59%3A59%22%20and%20gameDate%3E=%222000-10-01%22%20and%20gameTypeId=2",
    "faceoffpercentages": "https://api.nhle.com/stats/rest/en/skater/faceoffpercentages?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22totalFaceoffs%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=100&cayenneExp=gameDate%3C=%222000-10-31%2023%3A59%3A59%22%20and%20gameDate%3E=%222000-10-01%22%20and%20gameTypeId=2",
    "faceoffwins": "https://api.nhle.com/stats/rest/en/skater/faceoffwins?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22gameDate%22,%22direction%22:%22ASC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=50&cayenneExp=gameDate%3C=%222000-10-30%2023%3A59%3A59%22%20and%20gameDate%3E=%222000-10-01%22%20and%20gameTypeId=2",
    "goalsForAgainst": "https://api.nhle.com/stats/rest/en/skater/goalsForAgainst?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22gameDate%22,%22direction%22:%22ASC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=100&cayenneExp=gameDate%3C=%222000-10-31%2023%3A59%3A59%22%20and%20gameDate%3E=%222000-10-01%22%20and%20gameTypeId=2",
    "realtime": "https://api.nhle.com/stats/rest/en/skater/realtime?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22gameDate%22,%22direction%22:%22ASC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=100&cayenneExp=gameDate%3C=%222000-10-31%2023%3A59%3A59%22%20and%20gameDate%3E=%222000-10-01%22%20and%20gameTypeId=2",
    "penalties": "https://api.nhle.com/stats/rest/en/skater/penalties?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22penaltyMinutes%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=100&cayenneExp=gameDate%3C=%222000-10-31%2023%3A59%3A59%22%20and%20gameDate%3E=%222000-10-01%22%20and%20gameTypeId=2",
    "penaltykill": "https://api.nhle.com/stats/rest/en/skater/penaltykill?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22gameDate%22,%22direction%22:%22ASC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=100&cayenneExp=gameDate%3C=%222000-10-31%2023%3A59%3A59%22%20and%20gameDate%3E=%222000-10-01%22%20and%20gameTypeId=2",
    "penaltyShots": "https://api.nhle.com/stats/rest/en/skater/penaltyShots?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22gameDate%22,%22direction%22:%22ASC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=100&cayenneExp=gameDate%3C=%222000-10-31%2023%3A59%3A59%22%20and%20gameDate%3E=%222000-10-01%22%20and%20gameTypeId=2",
    "powerplay": "https://api.nhle.com/stats/rest/en/skater/powerplay?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22gameDate%22,%22direction%22:%22ASC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=100&cayenneExp=gameDate%3C=%222000-10-31%2023%3A59%3A59%22%20and%20gameDate%3E=%222000-10-01%22%20and%20gameTypeId=2",
    "puckPossessions": "https://api.nhle.com/stats/rest/en/skater/puckPossessions?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22gameDate%22,%22direction%22:%22ASC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=100&cayenneExp=gameDate%3C=%222000-10-31%2023%3A59%3A59%22%20and%20gameDate%3E=%222000-10-01%22%20and%20gameTypeId=2",
    "summaryshooting": "https://api.nhle.com/stats/rest/en/skater/summaryshooting?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22satTotal%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22usatTotal%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=100&cayenneExp=gameDate%3C=%222000-10-31%2023%3A59%3A59%22%20and%20gameDate%3E=%222000-10-01%22%20and%20gameTypeId=2",
    "percentages": "https://api.nhle.com/stats/rest/en/skater/percentages?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22satPercentage%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=100&cayenneExp=gameDate%3C=%222000-10-31%2023%3A59%3A59%22%20and%20gameDate%3E=%222000-10-01%22%20and%20gameTypeId=2",
    "scoringRates": "https://api.nhle.com/stats/rest/en/skater/scoringRates?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22pointsPer605v5%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22goalsPer605v5%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=100&cayenneExp=gameDate%3C=%222000-10-31%2023%3A59%3A59%22%20and%20gameDate%3E=%222000-10-01%22%20and%20gameTypeId=2",
    "scoringpergame": "https://api.nhle.com/stats/rest/en/skater/scoringpergame?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22gameDate%22,%22direction%22:%22ASC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=100&cayenneExp=gameDate%3C=%222000-10-31%2023%3A59%3A59%22%20and%20gameDate%3E=%222000-10-01%22%20and%20gameTypeId=2",
    "shootout": "https://api.nhle.com/stats/rest/en/skater/shootout?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22shootoutGoals%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=100&cayenneExp=gameDate%3C=%222000-10-31%2023%3A59%3A59%22%20and%20gameDate%3E=%222000-10-01%22%20and%20gameTypeId=2",
    "shottype": "https://api.nhle.com/stats/rest/en/skater/shottype?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22gameDate%22,%22direction%22:%22ASC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=100&cayenneExp=gameDate%3C=%222000-10-31%2023%3A59%3A59%22%20and%20gameDate%3E=%222000-10-01%22%20and%20gameTypeId=2",
    "timeonice": "https://api.nhle.com/stats/rest/en/skater/timeonice?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22gameDate%22,%22direction%22:%22ASC%22%7D,%7B%22property%22:%22playerId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=100&cayenneExp=gameDate%3C=%222000-10-31%2023%3A59%3A59%22%20and%20gameDate%3E=%222000-10-01%22%20and%20gameTypeId=2",
}
for k,v in url_dic.items():
    tempparms = tear_down_url(v)
    original_string = tempparms["cayenneExp"]

    # Starting Point
    base_str = original_string
    start_date = "2000-10-01"
    increment_type = "months"
    increment_value = 1
    iterations = 285

    # Generate the new date strings
    new_date_strings = increment_dates_in_string(
        base_str, start_date, increment_type, increment_value, iterations
    )

    for i, date_str in enumerate(new_date_strings):
        print(f"Type: {k} Iteration {i+1}: {date_str}")
        parms = tear_down_url(v)
        parms["cayenneExp"] = date_str
        new_url = rebuild_url(parms)
        # print(new_url)
        iterate_through_time_period(new_url, k)

