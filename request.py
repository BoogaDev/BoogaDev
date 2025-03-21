from packages.libs import *
from pandas.io.json import json_normalize
Logger().log()


# with pandas.option_context('display.max_rows', None, 'display.max_columns', None):
#     print(rawData)

# team_list_df = pandas.read_parquet('./temp/team_list/data.parquet.gzip')


def join_parquets(urlp):
    lp = os.listdir(urlp)
    # for x in lp:
    #     if x != '.DS_Store':
    #         print(x)
    #         if x != '2008':
    #             print()
    #             df = pandas.read_parquet(urlp+x+'/data.parquet.gzip')
    #             df.to_parquet(f'{urlp}{x}_data.parquet.gzip', compression='gzip')
    #             # df.to_parquet(f'{urlp}{x}_{os.listdir(os.path.join(urlp, x))[0]}', compression='gzip')
    #             # print(f'{urlp}{x}_{os.listdir(os.path.join(urlp, x))[0]}')
    # return
    base = pandas.DataFrame()
    for frame in lp:
        base = (
            pandas.read_parquet(os.path.join(urlp, frame))
            if base.empty
            else pandas.concat(
                [base, pandas.read_parquet(os.path.join(urlp, frame))], ignore_index=True
            )
        )
        os.remove(os.path.join(urlp, frame))
    if base.empty:
        os.rmdir(urlp)
        return False
    else:
        base.to_parquet(f'{urlp}/data.parquet.gzip', compression='gzip')
        return True




def schedule():
    date = pendulum.datetime(2023, 1, 1, 0)
    while date < pendulum.now():
        print(date.strftime("%Y-%m-%d"))
        time.sleep(.1)
        urlData = requests.get(f'https://statsapi.web.nhl.com/api/v1/schedule?expand=schedule.linescore&date={date.strftime("%Y-%m-%d")}')
        urlData = urlData.json()
        rawData = pandas.json_normalize(urlData, record_path=['dates', 'games'])
        if not rawData.empty:
            temp = pandas.json_normalize(rawData['linescore.periods'])
            result = temp.to_json(orient="records")
            parsed = json.loads(result)
            rawData = rawData.join(pandas.json_normalize(parsed))
            rawData.drop(columns=['linescore.periods',], inplace=True)
            # dpath = os.path.join('./temp/schedule/', date.strftime("%Y"))
            # try:
            #     os.makedirs(dpath)
            # except:
            #     pass
            rawData.to_parquet(f'./temp/schedule/{date.strftime("%Y_%m_%d")}_data.parquet.gzip', compression='gzip')
        # odate = date.strftime("%Y")
        date = date.add(days=1)
        # if odate != date.strftime("%Y") or date >= pendulum.now():
    join_parquets('./temp/schedule/')


schedule()

def team_list():
    d = requests.get('https://statsapi.web.nhl.com/api/v1/teams')
    urlData = d.json()
    rawData = pandas.json_normalize(urlData, record_path=['teams'])
    try:
        os.makedirs('./temp/team_list/')
    except:
        pass
    rawData.to_parquet('./temp/team_list/data.parquet.gzip', compression='gzip')


def team_lite_data():
    team_list_df = pandas.read_parquet('./temp/team_list/data.parquet.gzip')
    try:
        os.makedirs('./temp/team_lite_stats/')
        os.makedirs('./temp/team_lite_roster/')
    except:
        pass
    url = 'https://statsapi.web.nhl.com'+team_list_df['link'][i]
    for i in range(len(team_list_df['link'])):
        # Lite Team Stats
        d = requests.get(f'{url}/stats')
        urlData = d.json()
        rawData = pandas.json_normalize(urlData, record_path=['stats', ['splits']])
        rawData.drop(rawData.tail(1).index, inplace=True)
        rawData.to_parquet(f'./temp/team_lite_stats/{i}.parquet.gzip', compression='gzip')
        #Lite Team Roster
        d = requests.get(f'{url}/roster')
        urlData = d.json()
        rawData = pandas.json_normalize(urlData, record_path=['roster'])
        rawData.to_parquet(f'./temp/team_lite_roster/{i}.parquet.gzip', compression='gzip')
    
    join_parquets('./temp/team_lite_stats/')
    join_parquets('./temp/team_lite_roster/')





class SiteData():

    def __init__(self) -> None:
        self.teamurls = ['summary','daysbetweengames','faceoffpercentages','faceoffwins','goalsagainstbystrength','goalsbyperiod','goalsforbystrength','leadingtrailing','realtime','outshootoutshotby','penalties','penaltykill','penaltykilltime','powerplay','summaryshooting','scoretrailfirst','shootout','shottype','goalgames','powerplaytime','percentages']
        self.teambase_url = 'https://api.nhle.com/stats/rest/en/team/{0}?isAggregate=false&isGame=true&start={1}&limit=100&factCayenneExp=&cayenneExp=gameDate%3C=%22{3}%2023%3A59%3A59%22%20and%20gameDate%3E=%22{2}%22%20and%20gameTypeId=2'
        self.playerurls = ['summary', 'faceoffpercentages', 'faceoffwins', 'goalsForAgainst', 'realtime', 'penalties', 'penaltykill','powerplay', 'puckPossessions', 'summaryshooting', 'percentages', 'scoringRates', 'scoringpergame', 'shootout', 'shottype', 'timeonice']
        self.playerbase_url = 'https://api.nhle.com/stats/rest/en/skater/{0}?isAggregate=false&isGame=true&start={1}&limit=100&factCayenneExp=&cayenneExp=gameDate%3C=%22{3}%2023%3A59%3A59%22%20and%20gameDate%3E=%22{2}%22%20and%20gameTypeId=2'

    def generateDatesByMonth(self, date):
        dates = []
        for _ in range(180):
            dates.append([date.strftime("%Y-%m-%d"), date.add(months=1).subtract(days=1).strftime("%Y-%m-%d")])
            date = date.add(months=1)
            if date > pendulum.now():
                break
        return dates


    def pull(self, base_url, url, page, sd, cd, rtype):
        try:
            r = requests.get(base_url.format(url, page, sd, cd)).json()['data']
            rawData = pandas.json_normalize(r)
            if rawData.empty:
                return
            rawData.to_parquet(f'./temp/{rtype}/{url}/{sd}/{page}.parquet.gzip', compression='gzip')
        except Exception as e:
            logging.warning(e)


    def join_parquets(self, urlp):
        lp = os.listdir(urlp)
        base = pandas.DataFrame()
        for frame in lp:
            base = (
                pandas.read_parquet(os.path.join(urlp, frame))
                if base.empty
                else pandas.concat(
                    [base, pandas.read_parquet(os.path.join(urlp, frame))], ignore_index=True
                )
            )
            os.remove(os.path.join(urlp, frame))
        if base.empty:
            os.rmdir(urlp)
            return False
        else:
            base.to_parquet(f'{urlp}/data.parquet.gzip', compression='gzip')
            return True
        

    def requestData(self, urls, base_url, rtype, dates):
        for d in range(len(dates)):
            sd = dates[d][0]
            cd = dates[d][1]
            numrange = (pendulum.from_format(sd, 'YYYY-MM-DD').diff(pendulum.from_format(cd, 'YYYY-MM-DD')).in_days()+1)*260
            for url in urls:
                urlp = os.path.join('./temp/',rtype, url, sd)
                os.makedirs(urlp)
                print(f'Starting {url}')
                for page in range(0, numrange, 100):
                    time.sleep(.05)
                    threading.Thread(target=self.pull, args=[base_url, url, page, sd, cd, rtype]).start()
                while threading.active_count()>1:
                    time.sleep(.5)
                if not self.join_parquets(urlp):
                    break
    
    def fullSet(self, rtype=('player','team'), year=2008, month=7, day=1, urls = None, base_url = None):
        dates = self.generateDatesByMonth(pendulum.datetime(year, month, day, 0))
        if isinstance(rtype, list):
            self.requestData(self.playerurls, self.playerbase_url, rtype[0], dates)
            self.requestData(self.teamurls, self.teambase_url, rtype[1], dates)
        else:
            self.requestData(urls, base_url, rtype, dates)

        