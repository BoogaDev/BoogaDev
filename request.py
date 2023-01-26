from packages.libs import *
from pandas.io.json import json_normalize
Logger().log()

# urlData = requests.get('https://statsapi.web.nhl.com/api/v1/schedule')
# urlData = urlData.json()
# rawData = pandas.json_normalize(urlData, record_path=['dates','games'])
# with pandas.option_context('display.max_rows', None, 'display.max_columns', None):
#     print(rawData)

dt = pendulum.datetime(2022, 9, 1, 0)
numrange = (dt.diff(pendulum.now()).in_days()+1)*32
r = '2022-09-01'
d = datetime.datetime.strptime(r, '%Y-%m-%d')
sd = (d + datetime.timedelta(days=+1)).strftime("%Y-%m-%d")
cd = datetime.datetime.now().strftime("%Y-%m-%d")
urls = ['summary','daysbetweengames','faceoffpercentages','faceoffwins','goalsagainstbystrength','goalsbyperiod','goalsforbystrength','leadingtrailing','realtime','outshootoutshotby','penalties','penaltykill','penaltykilltime','powerplay','powerplaytime','summaryshooting','percentages','scoretrailfirst','shootout','shottype','goalgames']
base_url = 'https://api.nhle.com/stats/rest/en/team/{0}?isAggregate=false&isGame=true&start={1}&limit=100&factCayenneExp=gamesPlayed%3E=1&cayenneExp=gameDate%3C=%22{3}%2023%3A59%3A59%22%20and%20gameDate%3E=%22{2}%22%20and%20gameTypeId=2'

def pull(url, page, sd, cd):
    try:
        r = requests.get(base_url.format(url, page, sd, cd)).json()['data']
        rawData = pandas.json_normalize(r)
        if rawData.empty:
            return
        rawData.to_parquet(f'./temp/{url}_{page}.parquet.gzip', compression='gzip')
    except Exception as e:
        logging.warning(e)

for url in urls:
    print(numrange)
    for page in range(0, numrange, 100):
        threading.Thread(target=pull, args=[url, page, sd, cd]).start()

        
