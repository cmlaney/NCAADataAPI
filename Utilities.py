import sqlite3
from Models import Play, Possession
import re

conn = sqlite3.connect("games.sqlite")
c = conn.cursor()

def setHomeAwayInPoss():
    c.execute('select game_id, team_name from possession')
    for row in c.fetchall():
        c.execute('select home_name from game where id={}'.format(row[0]))
        home = 0
        if c.fetchone()[0] == row[1]:
            home = 1
        c.execute('update possession set home={} where game_id={} and team_name="{}"'.format(home, row[0], row[1]))

def getPossesions(game_id):
    c.execute('select home_name, visiting_name from game where id = {}'.format(game_id))
    row = c.fetchone()
    homeTeam = row[0]
    visitingTeam = row[1]
    teams = [homeTeam, visitingTeam]

    possessions = []

    for team in teams:
        c.execute('select possession.id, time, period, start_date from possession join game on possession.game_id '
                      '= game.id where game.id={} and team_name="{}"'.format(game_id, team))

        for row in c.fetchall():
            id = row[0]
            time = row[1]
            period = row[2]
            date = row[3]
            c.execute('select text, drive, hScore, vScore, rowid from play where possession_id={}'.format(id))
            plays = []
            for play in c.fetchall():
                plays.append(Play(play[0], play[1], play[3], play[2], play[4]))
            possessions.append(Possession(team, time, plays, period, id))

    return possessions

def updatePlayScores(game_id):
    periods = {}
    for possession in getPossesions(game_id):
        if possession.period not in periods:
            periods[possession.period] = {}
        time = possession.time
        if time[0] == ':':
            time = '00'+time
        periods[possession.period][time] = possession

    currentHScore = 0
    currentVScore = 0
    periodList = list(periods.keys())
    periodList.sort()
    for period in periodList:
        possessionTimes = list(periods[period].keys())
        possessionTimes.sort()
        possessionTimes.reverse()
        for time in possessionTimes:
            #print('Period: {} - Time: {} - Team: {}'.format(period, time, periods[period][time].teamName))
            for play in periods[period][time].plays:
                if play.vScore > currentVScore:
                    currentVScore = play.vScore
                if play.hScore > currentHScore:
                    currentHScore = play.hScore
                if play.hScore == -1:
                    play.hScore = currentHScore
                if play.vScore == -1:
                    play.vScore = currentVScore
                c.execute('update play set hScore={}, vScore={} where rowid={}'.format(play.hScore, play.vScore, play.id))

def profilePlay(play, team):
    c.execute('select short_name from team where team_name="{}"'.format(team))
    shortName = c.fetchone()[0]

    if 'INTERCEPTED' in play.text:
        play.intercepted = True
    if 'FUMBLES' in play.text:
        play.fumbled = True
    if 'Penalty' in play.text:
        play.penalty = True
    if 'sacked' in play.text:
        play.sacked = True
    if 'touchdown' in play.text:
        play.touchdown = True
    if 'extra point' in play.text:
        play.extra_point = True
        if 'good' in play.text:
            play.xp_good = True
    if 'Field Goal' in play.text:
        play.field_goal = True
        param = re.findall('[0-9]+ yard', play.text)
        if len(param) > 0:
            comp = param[0].strip().split()
            play.field_goal_yds = int(comp[0])
    if 'safety' in play.text:
        play.safety = True
    if ' complete' in play.text:
        play.passing = True
        play.completion = True
        param = re.findall('to [A-Z]+ [0-9]+', play.text)
        if len(param) > 0:
            comp = param[0].strip().split()
            if shortName == comp[1]:
                play.field_pos = 100 - int(comp[2])
            else:
                play.field_pos = int(comp[2])
        param = re.findall('for [0-9]+ yard', play.text)
        if len(param) > 0:
            comp = param[0].strip().split()
            play.yds = int(comp[-2])
        if 'touchdown' in play.text:
            param = re.findall('runs [0-9]+ yard', play.text)
            if len(param) > 0:
                comp = param[0].strip().split()
                play.yds = int(comp[-2])
    if 'incomplete' in play.text:
        play.passing = True
    if 'kicks' in play.text:
        play.kick_ret = True
        if 'touchback' in play.text:
            play.touchback = True
            play.field_pos = 20
        elif 'touchdown' in play.text:
            play.touchdown = True
            play.field_pos = 0
            kick_param = re.findall('runs [0-9]+ yard', play.text)
            if len(kick_param) > 0:
                components = kick_param[0].strip().split()
                play.kick_ret_yds = components[1]
        else:
            kick_param = re.findall('to [A-Z]+ [0-9]+ for [0-9]+ yard', play.text)
            if len(kick_param) > 0:
                components = kick_param[0].strip().split()
                if components[2] == shortName:
                    play.field_pos = 100 - int(components[2])
                else:
                    play.field_pos = int(components[2])
                play.kick_ret_yds = int(components[4])
    if 'punts' in play.text:
        play.punt = True
        param = re.findall('punts [0-9]+ yard', play.text)
        if len(param) > 0:
            comp = param[0].strip().split()
            play.punt_yds = int(comp[1])
        yd_param = re.findall('to (?:the)? [A-Z]+ [0-9]+', play.text)
        if len(yd_param) > 0:
            comp = yd_param[0].strip().split()
            if shortName == comp[-2]:
                play.field_pos = int(comp[-1])
            else:
                play.field_pos = 100 - int(comp[-1])
        if 'touchback' in play.text:
            play.touchback = True
        if 'touchdown' in play.text:
            play.touchdown = True
        if 'fair catch' in play.text:
            play.fair_catch = True
        if 'downed' in play.text:
            play.downed = True
    if not (play.passing or play.punt or play.kick_ret or play.extra_point or play.field_goal):
        play.rushing = True
        if 'touchdown' in play.text:
            play.touchdown = True
            param = re.findall('runs [0-9]+ yard', play.text)
            if len(param) > 0:
                comp = param[0].strip().split()
                play.yds = int(comp[1])
        else:
            param = re.findall('for [-?0-9]+ yard', play.text)
            if len(param) > 0:
                comp = param[0].strip().split()
                play.yds = int(comp[-2])
                if play.safety:
                    play.field_pos = 100
                else:
                    param = re.findall('[to|at] [A-Z]+ [0-9]+', play.text)
                    if len(param) > 0:
                        comp = param[0].strip().split()
                        if shortName == comp[1]:
                            play.field_pos = 100 - int(comp[2])
                        else:
                            play.field_pos = int(comp[2])

def updatePlay(play):
    sql = 'update play set '
    execute = play.passing or play.punt or play.kick_ret or play.extra_point or play.field_goal or play.rushing
    args = []
    if play.touchdown:
        args.append('touchdown=1')
    if play.intercepted:
        args.append('intercepted=1')
    if play.safety:
        args.append('safety=1')
    if play.fumbled:
        args.append('fumbled=1')
    if play.passing:
        args.append('passing=1')
        if play.completion:
            args.append('completion=1')
        args.append('yds={}'.format(play.yds))
    if play.rushing:
        args.append('rushing=1')
        args.append('yds={}'.format(play.yds))
    if play.sacked:
        args.append('sacked=1')
    if play.punt:
        args.append('punt=1')
        args.append('punt_yds={}'.format(play.punt_yds))
        if play.fair_catch:
            args.append('fair_catch=1')
        if play.downed:
            args.append('downed=1')
    if play.field_goal:
        args.append('field_goal=1')
        args.append('field_goal_yds={}'.format(play.field_goal_yds))
    if play.kick_ret:
        args.append('kick_ret=1')
        args.append('kick_ret_yds={}'.format(play.kick_ret_yds))
    if play.touchback:
        args.append('touchback=1')
    if play.field_pos > 0:
        args.append('field_pos={}'.format(play.field_pos))
    if play.extra_point:
        args.append('extra_point=1')
        if play.xp_good:
            args.append('xp_good=1')

    first = True
    if len(args) > 0:
        for arg in args:
            if first:
                first = False
                sql += arg
            else:
                sql += ', ' + arg

    sql += ' where rowid = {}'.format(play.id)
    if execute:
        c.execute(sql)

def determineShortNames():
    c.execute('select id from game')
    games = []
    for row in c.fetchall():
        games.append(row[0])
    for game in games:
        teams = {}
        possessions = getPossesions(game)
        for possession in possessions:
            if possession.teamName not in teams:
                teams[possession.teamName] = []
            teams[possession.teamName].append(possession)
        for team, possessions in teams.items():
            shortName = ''
            otherTeam = list(teams.keys())
            otherTeam.remove(team)
            otherTeam = otherTeam[0]
            for possession in possessions:
                for play in possession.plays:
                    if 'kicks' in play.text:
                        parts = re.findall('kicks [0-9]+ yards from [A-Z]+ [0-9]+', play.text)
                        split = parts[0].strip().split()
                        shortName = split[-2]
                        print(shortName)
                        break
                if len(shortName) > 0:
                    break
            c.execute('update team set short_name="{}" where team_name="{}"'.format(shortName, otherTeam))

def fixNulls():
    cols = ['intercepted', 'fumbled', 'touchdown', 'passing', 'rushing', 'completion', 'punt', 'punt_yds', 'fair_catch', 'downed',
            'kick_ret', 'kick_ret_yds', 'field_pos', 'touchback', 'yds', 'extra_point', 'xp_good', 'sacked', 'field_goal',
            'field_goal_yds', 'safety']
    for col in cols:
        c.execute('update play set {}=0 where {} is NULL'.format(col, col))

def removeDuplicateGames():
    c.execute('select id, game_url from game')
    games = []
    gamesToRemove = []
    results = c.fetchall()
    for row in results:
        if row[1] not in games:
            games.append(row[1])
        else:
            gamesToRemove.append(row[0])
    print(gamesToRemove)
    for game_id in gamesToRemove:
        # remove possession and plays
        c.execute('select id from possession where game_id={}'.format(game_id))
        for row in c.fetchall():
            c.execute('delete from play where possession_id={}'.format(row[0]))
            c.execute('delete from possession where id={}'.format(row[0]))
        # remove score
        c.execute('delete from score where game_id={}'.format(game_id))
        # remove stats
        c.execute('select id from stats where game_id={}'.format(game_id))
        for row in c.fetchall():
            stats = ['kick', 'kick_ret', 'punt', 'punt_return', 'pass', 'rush', 'recv']
            for stat in stats:
                c.execute('delete from {} where stats_id={}'.format(stat, row[0]))
        # remove team_stats
        c.execute('delete from team_stats where game_id={}'.format(game_id))

        c.execute('delete from game where id={}'.format(game_id))

conn.commit()
conn.close()