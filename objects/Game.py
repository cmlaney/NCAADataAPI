import json, requests

debug = False

class Game(object):

    def __init__(self, id, updatedTimestamp, conference, gameState, startDate, startTimeEpoch, currentPeriod, finalMessage, \
                 gameStatus, periodStatus, downToGo, timeclock, location, scoreBreakdown, home, away, tabsArray, url):

        if debug:
            print(id)
        self.id = id
        if debug:
            print(url)
        self.url = url
        if debug:
            print(updatedTimestamp)
        self.updatedTimestamp = updatedTimestamp
        if debug:
            print(conference)
        self.conference = conference
        if debug:
            print(gameState)
        self.gameState = gameState
        if debug:
            print(startDate)
        self.startDate = startDate
        if debug:
            print(startTimeEpoch)
        self.startTimeEpoch = startTimeEpoch
        if debug:
            print(currentPeriod)
        self.currentPeriod = currentPeriod
        if debug:
            print(finalMessage)
        self.finalMessage = finalMessage
        if debug:
            print(gameStatus)
        self.gameStatus = gameStatus
        if debug:
            print(periodStatus)
        self.periodStatus = periodStatus
        if debug:
            print(downToGo)
        self.downToGo = downToGo
        if debug:
            print(timeclock)
        self.timeclock = timeclock
        if debug:
            print(location)
        self.location = location
        if debug:
            print(str(scoreBreakdown))
        self.scoreBreakdown = scoreBreakdown

        self.homeTeam = TeamInfo(home['teamRank'], home['iconURL'], home['nameRaw'], home['color'], home['description'], \
                                 home['currentScore'], home['scoreBreakdown'])
        self.awayTeam = TeamInfo(away['teamRank'], away['iconURL'], away['nameRaw'], away['color'], away['description'], \
                                 away['currentScore'], away['scoreBreakdown'])
        self.winningTeam = self.homeTeam
        if away['winner'] == 'true':
            self.winningTeam = self.awayTeam
        self.tabs = {}
        for tab in tabsArray:
            for subtab in tab:
                self.tabs[subtab['type']] = subtab['file']
        if 'recap' in self.tabs:
            self.recap = Recap('http://data.ncaa.com' + str(self.tabs['recap']))
        if 'boxscore' in self.tabs:
            self.boxscore = Boxscore('http://data.ncaa.com' + str(self.tabs['boxscore']))
        if 'scoring-summary' in self.tabs:
            self.scoringSummary = ScoringSummary('http://data.ncaa.com' + str(self.tabs['scoring-summary']))
        if 'team-stats' in self.tabs:
            self.teamStats = TeamStats('http://data.ncaa.com' + str(self.tabs['team-stats']))
        if 'play-by-play' in self.tabs:
            self.playByPlay = PlayByPlay('http://data.ncaa.com' + str(self.tabs['play-by-play']))

    def __str__(self):
        return ('id: {}\ntimestamp: {}\nconference: {}\ngame state: {}\nstart date: {}\nstart time: {}\n'\
                + 'current period: {}\nfinal: {}\nstatus: {}\nperiod status: {}\ndown to go: {}\n'\
                + 'time clock: {}\nlocation: {}\nscoring: {}\nhome: {}\naway: {}\nwinner: {}')\
            .format(self.id, self.updatedTimestamp, self.conference, self.gameState, self.startDate, \
                    self.startTimeEpoch, self.currentPeriod, self.finalMessage, self.gameStatus, \
                    self.periodStatus, self.downToGo, self.timeclock, self.location, self.scoreBreakdown, self.homeTeam, \
                    self.awayTeam, self.winningTeam.nameRaw)

    def getTabs(self):
        return self.tabs


class TeamInfo(object):

    def __init__(self, rank, iconURL, nameRaw, color, record, currentScore, scoreBreakdown):
        self.rank = rank
        self.iconURL = iconURL
        self.nameRaw = nameRaw
        self.color = color
        self.record = record
        self.currentScore = currentScore
        self.scoreBreakdown = scoreBreakdown

    def __str__(self):
        return ('\n\trank: {}\n\tname raw: {}\n\tcolor: {}\n\trecord: {}\n\tscore: {}\n\tscore breakdown: {}') \
            .format(self.rank, self.nameRaw, self.color, self.record, self.currentScore, \
                    self.scoreBreakdown)

class Recap(object):

    def __init__(self, url):
        resp = requests.get(url=url, timeout=15)
        data = json.loads(resp.content)
        self.title = data['title']
        self.content = data['content'].replace('&nbsp;', ' ').replace('<p>', '\n').replace('</p>', '').replace('<li>', '\n')\
        .replace('</li>', '').replace('<ul>', '').replace('</ul>', '').replace('&rsquo;', '\'').replace('<strong>', '\n')\
        .replace('</strong>', '')

    def __str__(self):
        return '{}\n{}'.format(self.title, self.content)

class Boxscore(object):

    def __init__(self, url):
        resp = requests.get(url=url, timeout=15)
        data = json.loads(resp.content)
        self.tables = {}
        for table in data['tables']:
            tableId = table['id']
            tableDict = {}
            header = table['header']
            headList = []
            for entry in header:
                headList.append(entry['display'])
            tableData = table['data']
            for entry in tableData:
                dataList = []
                for val in entry['row']:
                    dataList.append(val['display'])
                tableDict[dataList[0]] = dict(zip(headList, dataList))
            self.tables[tableId] = tableDict

    def __str__(self):
        output = ''
        for id in self.tables.keys():
            output += id + '\n'
            for row in self.tables[id]:
                output += str(self.tables[id][row]) + '\n'
            output += '\n'
        return output

class ScoringSummary(object):

    def __init__(self, url):
        resp = requests.get(url=url, timeout=15)
        data = json.loads(resp.content)
        teams = {}
        for team in data['meta']['teams']:
            teams[team['id']] = team['shortname']
        self.periods = {}
        for period in data['periods']:
            scores = []
            for score in period['summary']:
                newScore = Score(teams[score['teamId']], score['time'], score['scoreType'], score['scoreText'], \
                                 score['driveText'], score['visitingScore'], score['homeScore'])
                scores.append(newScore)
            self.periods[period['title']] = scores

    def __str__(self):
        output = ''
        for period, scores in self.periods.iteritems():
            output += period + '\n\n'
            for score in scores:
                output += str(score) + '\n'
            output += '\n'
        return output

class Score(object):

    def __init__(self, teamName, time, type, text, drive, visitorScore, homeScore):
        self.teamName = teamName
        self.time = time
        self.type = type
        self.text = text
        self.drive = drive
        self.visitorScore = visitorScore
        self.homeScore = homeScore

    def __str__(self):
        return 'Team: {}\t{}\t{}\n- {}\n- {}\nHome: {}\tVisitor: {}'.format(self.teamName, self.time, self.type, \
                                                        self.text, self.drive, self.homeScore, self.visitorScore)

class TeamStats(object):

    def __init__(self, url):
        resp = requests.get(url=url, timeout=15)
        data = json.loads(resp.content)
        teams = {}
        self.stats = {}
        for team in data['meta']['teams']:
            teams[team['id']] = team['shortname']
        for team in data['teams']:
            self.stats[teams[team['teamId']]] = {}
            for stat in team['stats']:
                newStat = {}
                if 'data' in stat:
                    newStat['data'] = stat['data']
                if 'breakdown' in stat:
                    breakdown = {}
                    for subStat in stat['breakdown']:
                        breakdown[subStat['stat']] = subStat['data']
                    newStat['breakdown'] = breakdown
                self.stats[teams[team['teamId']]][stat['stat']] = newStat

    def __str__(self):
        output = ''
        for team, stats in self.stats.iteritems():
            output += team + '\n'
            for stat, dict in stats.iteritems():
                output += stat + '\n'
                for key, value in dict.iteritems():
                    output += key + ' : ' + str(value) + '\n'
            output += '\n'
        return output

class PlayByPlay(object):

    def __init__(self, url):
        resp = requests.get(url=url, timeout=15)
        data = json.loads(resp.content)
        teams = {}
        self.periods = {}
        for team in data['meta']['teams']:
            teams[team['id']] = team['shortname']
        for period in data['periods']:
            self.periods[period['title']] = []
            for possession in period['possessions']:
                newPossession = Possession(teams[possession['teamId']], possession['time'], possession['plays'])
                self.periods[period['title']].append(newPossession)

    def __str__(self):
        output = ''
        for period, possessions in self.periods.iteritems():
            output += period + '\n'
            for possession in possessions:
                output += str(possession)
        return output

class Possession(object):

    def __init__(self, teamName, time, plays, id=-1):
        self.teamName = teamName
        self.time = time
        self.plays = []
        self.id = id
        lastVScore = -1
        lastHScore = -1
        for play in plays:
            if len(play['visitingScore']) > 0:
                lastVScore = int(play['visitingScore'])
            if len(play['homeScore']) > 0:
                lastHScore = int(play['homeScore'])
            newPlay = Play(play['scoreText'], play['driveText'], lastVScore, lastHScore, -1)
            self.plays.append(newPlay)

    def __str__(self):
        output = ''
        output += self.teamName + '\n'
        output += self.time + '\n'
        for play in self.plays:
            output += str(play) + '\n'
        return output

class Play(object):

    def __init__(self, text, drive, vScore, hScore, rowid):
        self.id = rowid
        try:
            test = text.encode('ascii')
            self.text = text
        except UnicodeEncodeError:
            self.text = 'Unable to parse text'
            pass
        try:
            self.drive = drive
        except UnicodeEncodeError:
            self.drive = 'Unable to parse drive'
            pass
        self.vScore = vScore
        self.hScore = hScore

        self.intercepted = False
        self.fumbled = False
        self.touchdown = False
        self.passing = False
        self.rushing = False
        self.completion = False
        self.punt = False
        self.punt_yds = 0
        self.fair_catch = False
        self.downed = False
        self.kick_ret = False
        self.kick_ret_yds = 0
        self.field_pos = 0
        self.touchback = False
        self.yds = 0
        self.extra_point = False
        self.xp_good = True
        self.sacked = False
        self.field_goal = False
        self.field_goal_yds = 0
        self.safety = False

    def __str__(self):
        breakdown = ''
        if self.passing:
            breakdown = 'Passing: '
            if self.completion:
                breakdown += 'complete, for {} yards, {} to go'.format(self.yds, self.field_pos)
                if self.touchdown:
                    breakdown += ', touchdown'
            else:
                breakdown += 'incomplete'
            if self.intercepted:
                breakdown += ', intercepted'
            elif self.fumbled:
                breakdown += ', fumbled'
            if self.touchdown:
                breakdown += ', touchdown'
        elif self.rushing:
            breakdown = 'Rushing: for {} yards, {} to go'.format(self.yds, self.field_pos)
            if self.fumbled:
                breakdown += ', fumble'
            if self.touchdown:
                breakdown += ', touchdown'
            if self.sacked:
                breakdown += ', sacked'
        elif self.kick_ret:
            breakdown = 'Kick Return: '
            if self.touchback:
                breakdown += 'touchback'
            else:
                breakdown += 'returned for {} yards, {} to go'.format(self.kick_ret_yds, self.field_pos)
                if self.touchdown:
                    breakdown += ', touchdown'
                elif self.fumbled:
                    breakdown += ', fumbled'
        elif self.punt:
            breakdown = 'Punt: yards - {}'.format(self.punt_yds)
            if self.fair_catch:
                breakdown += ', fair catch'
            elif self.downed:
                breakdown += ', downed'
        elif self.extra_point:
            breakdown = 'Extra Point: is '
            if not self.xp_good:
                breakdown += 'no '
            breakdown += 'good'
        elif self.field_goal:
            breakdown = 'Field Goal: {} yards'.format(self.field_goal_yds)


        try:
            return '{}\n\n{}, Score - Home: {}\tAway: {}\n'.format(self.text, breakdown, self.hScore, self.vScore)
        except UnicodeEncodeError:
            return '{}\n{}\nHome: {}\tAway: {}'.format('Unable to parse text', self.drive, self.hScore, self.vScore)
            pass