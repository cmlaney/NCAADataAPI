
import sqlite3
from Models import Play, Possession
import re

connGame = sqlite3.connect("games.sqlite")
cGame = connGame.cursor()
cPlay = connGame.cursor()

def buildStats(team):
    cGame.execute('select stat_3rd_down_att, stat_3rd_down_conv, off_yds, avg_yds_per_play, off_plays, \
                              stat_4th_down_att, stat_4th_down_conv, stat_1st_downs_total, stat_1st_downs_from_penalty,\
                              stat_1st_down_pass, stat_1st_down_rush, penalty_num, penalty_yards, int_recov_num, \
                              int_recov_yds, punt_ret_num, punt_ret_yds, pass_att, pass_comp, pass_yds_total, \
                              pass_yds_avg, pass_int_thrown, kick_ret_num, kick_ret_yds, rush_num, rush_yds_total, \
                              rush_yds_avg, fum_num, fum_lost, punt_num, punt_yds, start_date from team_stats \
                              join game on team_stats.game_id = game.id where team_name="{}" order by start_date'.format(team))

    stat_3rd_down_att = []
    stat_3rd_down_conv = []
    off_yds = []
    avg_yds_per_play = []
    off_plays = []
    stat_4th_down_att = []
    stat_4th_down_conv = []
    stat_1st_downs_total = []
    stat_1st_downs_from_penalty = []
    stat_1st_down_pass = []
    stat_1st_down_rush = []
    penalty_num = []
    penalty_yards = []
    int_recov_num = []
    int_recov_yds = []
    punt_ret_num = []
    punt_ret_yds = []
    pass_att = []
    pass_comp = []
    pass_yds_total = []
    pass_yds_avg = []
    pass_int_thrown = []
    kick_ret_num = []
    kick_ret_yds = []
    rush_num = []
    rush_yds_total = []
    rush_yds_avg = []
    fum_num = []
    fum_lost = []
    punt_num = []
    punt_yds = []
    date = []

    for row in cGame.fetchall():
        stat_3rd_down_att.append(row[0])
        stat_3rd_down_conv.append(row[1])
        off_yds.append(row[2])
        avg_yds_per_play.append(row[3])
        off_plays.append(row[4])
        stat_4th_down_att.append(row[5])
        stat_4th_down_conv.append(row[6])
        stat_1st_downs_total.append(row[7])
        stat_1st_downs_from_penalty.append(row[8])
        stat_1st_down_pass.append(row[9])
        stat_1st_down_rush.append(row[10])
        penalty_num.append(row[11])
        penalty_yards.append(row[12])
        int_recov_num.append(row[13])
        int_recov_yds.append(row[14])
        punt_ret_num.append(row[15])
        punt_ret_yds.append(row[16])
        pass_att.append(row[17])
        pass_comp.append(row[18])
        pass_yds_total.append(row[19])
        pass_yds_avg.append(row[20])
        pass_int_thrown.append(row[21])
        kick_ret_num.append(row[22])
        kick_ret_yds.append(row[23])
        rush_num.append(row[24])
        rush_yds_total.append(row[25])
        rush_yds_avg.append(row[26])
        fum_num.append(row[27])
        fum_lost.append(row[28])
        punt_num.append(row[29])
        punt_yds.append(row[30])
        date.append(row[31])



    stats = {
        'stat_3rd_down_att' : stat_3rd_down_att,
        'stat_3rd_down_conv': stat_3rd_down_conv,
        'off_yds': off_yds,
        'avg_yds_per_play': avg_yds_per_play,
        'off_plays': off_plays,
        'stat_4th_down_att': stat_4th_down_att,
        'stat_4th_down_conv': stat_4th_down_conv,
        'stat_1st_downs_total': stat_1st_downs_total,
        'stat_1st_downs_from_penalty': stat_1st_downs_from_penalty,
        'stat_1st_down_pass': stat_1st_down_pass,
        'stat_1st_down_rush': stat_1st_down_rush,
        'penalty_num': penalty_num,
        'penalty_yards': penalty_yards,
        'int_recov_num': int_recov_num,
        'int_recov_yds': int_recov_yds,
        'punt_ret_num': punt_ret_num,
        'punt_ret_yds': punt_ret_yds,
        'pass_att': pass_att,
        'pass_comp': pass_comp,
        'pass_yds_total': pass_yds_total,
        'pass_yds_avg': pass_yds_avg,
        'pass_int_thrown': pass_int_thrown,
        'kick_ret_num': kick_ret_num,
        'kick_ret_yds': kick_ret_yds,
        'rush_num': rush_num,
        'rush_yds_total': rush_yds_total,
        'rush_yds_avg': rush_yds_avg,
        'fum_num': fum_num,
        'fum_lost': fum_lost,
        'punt_num': punt_num,
        'punt_yds': punt_yds,
        'date': date
    }

    return stats

def getPlays(team):
    cGame.execute('select possession.id, time, period, start_date from possession join game on possession.game_id = game.id where \
        team_name="{}" order by start_date'.format(team))

    dates = {}

    for row in cGame.fetchall():
        id = row[0]
        time = row[1]
        period = row[2]
        date = row[3]
        if date not in dates:
            dates[date] = []
        cPlay.execute('select text, drive, hScore, vScore, rowid from play where possession_id={}'.format(id))
        plays = []
        for play in cPlay.fetchall():
            plays.append(Play(play[0], play[1], play[3], play[2], play[4]))
            profilePlay(plays[-1], team)
            #if 'to NEB' in plays[-1].text:
                #print(plays[-1].text)
        dates[date].append(Possession(team, time, plays, period, id))

    return dates

def getPossesions(game_id):
    cGame.execute('select home_name, visiting_name from game where id = {}'.format(game_id))
    row = cGame.fetchone()
    homeTeam = row[0]
    visitingTeam = row[1]
    teams = [homeTeam, visitingTeam]

    possessions = []

    for team in teams:
        cGame.execute('select possession.id, time, period, start_date from possession join game on possession.game_id '
                      '= game.id where game.id={} and team_name="{}"'.format(game_id, team))

        for row in cGame.fetchall():
            id = row[0]
            time = row[1]
            period = row[2]
            date = row[3]
            cPlay.execute('select text, drive, hScore, vScore, rowid from play where possession_id={}'.format(id))
            plays = []
            for play in cPlay.fetchall():
                plays.append(Play(play[0], play[1], play[3], play[2], play[4]))
            possessions.append(Possession(team, time, plays, period, id))

    return possessions



def getLastXGameAvg(stat, games=1):
    avg = 0;
    count = 0;
    while count <= games:
        pos = -1 * count
        avg += float(stat[pos])
        count += 1
    avg = avg/(count - 1)
    return avg

def getBigPlays(team):
    dates = getPlays(team)
    bigPlays = []
    for date, possessions in dates.items():
        # print(date)
        for possession in possessions:
            # print(possession)
            for play in possession.plays:
                if 'INTERCEPTED' not in play.text and 'FUMBLES' not in play.text:
                    if 'complete' in play.text:
                        text = re.findall('for [0-9]+ yards', play.text)
                        if len(text) > 0:
                            yardage = re.findall('[0-9]+', text[0])
                            if int(yardage[0]) > 10:
                                bigPlays.append(('pass - pure', date, possession.period, yardage[0], play.text, play.hScore, play.vScore))
                        else:
                            text = re.findall('runs [0-9]+ yards', play.text)
                            if len(text) > 0:
                                yardage = re.findall('[0-9]+', text[0])
                                if int(yardage[0]) > 10:
                                    bigPlays.append(
                                        ('pass - caught and ran', date, possession.period, yardage[0], play.text, play.hScore, play.vScore))
                    elif 'kicks' not in play.text and 'punts' not in play.text and 'Holding' not in play.text:
                        text = re.findall('to [A-Z]+ [0-9]{1,2} for [0-9]+ yards', play.text)
                        if len(text) > 0:
                            yardage = re.findall('for [0-9]+ yards', text[0])
                            yardStr = yardage[0].strip().split()
                            yds = yardStr[1]
                            if int(yds) > 10:
                                bigPlays.append(
                                    ('rush', date, possession.period, yds, play.text,
                                     play.hScore, play.vScore))
    return bigPlays


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
                cGame.execute('update play set hScore={}, vScore={} where rowid={}'.format(play.hScore, play.vScore, play.id))

def profilePlay(play, team):
    cGame.execute('select short_name from team where team_name="{}"'.format(team))
    shortName = cGame.fetchone()[0]
    intercepted = False
    fumbled = False
    touchdown = False
    passing = False
    rushing = False
    completion = False
    punt = False
    punt_yds = False
    fair_catch = False
    downed = False
    kick_ret = False
    kick_ret_yds = 0
    field_pos = 0
    touchback = False
    yds = 0
    extra_point = False
    xp_good = False
    sacked = False
    field_goal = False
    field_goal_yds = 0

    if 'INTERCEPTED' in play.text:
        intercepted = True
    if 'FUMBLES' in play.text:
        fumbled = True
    if 'Penalty' in play.text:
        penalty = True
    if 'sacked' in play.text:
        sacked = True
    if 'touchdown' in play.text:
        touchdown = True
    if 'extra point' in play.text:
        extra_point = True
        if 'good' in play.text:
            xp_good = True
    if 'Field Goal' in play.text:
        field_goal = True
        param = re.findall('[0-9]+ yard', play.text)
        if len(param) > 0:
            comp = param[0].strip().split()
            field_goal_yds = int(comp[0])
    if ' complete' in play.text:
        passing = True
        completion = True
        param = re.findall('to [A-Z]+ [0-9]+', play.text)
        if len(param) > 0:
            comp = param[0].strip().split()
            if shortName == comp[1]:
                field_pos = 100 - int(comp[2])
            else:
                field_pos = int(comp[2])
        param = re.findall('for [0-9]+ yard', play.text)
        if len(param) > 0:
            comp = param[0].strip().split()
            yds = int(comp[-2])
        if 'touchdown' in play.text:
            param = re.findall('runs [0-9]+ yard', play.text)
            if len(param) > 0:
                comp = param[0].strip().split()
                yds = int(comp[-2])
    if 'incomplete' in play.text:
        passing = True
    if 'kicks' in play.text:
        kick_ret = True
        if 'touchback' in play.text:
            touchback = True
            field_pos = 20
        elif 'touchdown' in play.text:
            touchdown = True
            field_pos = 0
            kick_param = re.findall('runs [0-9]+ yard', play.text)
            if len(kick_param) > 0:
                components = kick_param[0].strip().split()
                kick_ret_yds = components[1]
        else:
            kick_param = re.findall('to [A-Z]+ [0-9]+ for [0-9]+ yard', play.text)
            if len(kick_param) > 0:
                components = kick_param[0].strip().split()
                if components[2] == shortName:
                    field_pos = 100 - int(components[2])
                else:
                    field_pos = int(components[2])
                kick_ret_yds = int(components[4])
    if 'punts' in play.text:
        punt = True
        param = re.findall('punts [0-9]+ yard', play.text)
        if len(param) > 0:
            comp = param[0].strip().split()
            punt_yds = int(comp[1])
        yd_param = re.findall('to (?:the)? [A-Z]+ [0-9]+', play.text)
        if len(yd_param) > 0:
            comp = yd_param[0].strip().split()
            if shortName == comp[-2]:
                field_pos = int(comp[-1])
            else:
                field_pos = 100 - int(comp[-1])
        if 'touchback' in play.text:
            touchback = True
        if 'touchdown' in play.text:
            touchdown = True
        if 'fair catch' in play.text:
            fair_catch = True
        if 'downed' in play.text:
            downed = True
    if not (passing or punt or kick_ret or extra_point or field_goal):
        rushing = True
        if 'touchdown' in play.text:
            touchdown = True
            param = re.findall('runs [0-9]+ yard', play.text)
            if len(param) > 0:
                comp = param[0].strip().split()
                yds = int(comp[1])
        else:
            param = re.findall('[to|at] [A-Z]+ [0-9]+ for [-?0-9]+ yard', play.text)
            if len(param) > 0:
                comp = param[0].strip().split()
                yds = int(comp[-2])
                if shortName == comp[1]:
                    field_pos = 100 - int(comp[2])
                else:
                    field_pos = int(comp[2])
    play.intercepted = intercepted
    play.fumbled = fumbled
    play.touchdown = touchdown
    play.passing = passing
    play.rushing = rushing
    play.completion = completion
    play.punt = punt
    play.punt_yds = punt_yds
    play.fair_catch = fair_catch
    play.downed = downed
    play.kick_ret = kick_ret
    play.kick_ret_yds = kick_ret_yds
    play.field_pos = field_pos
    play.touchback = touchback
    play.yds = yds
    play.extra_point = extra_point
    play.xp_good = xp_good
    play.sacked = sacked
    play.field_goal = field_goal
    play.field_goal_yds = field_goal_yds

def printBigPlays(team):
    bigs = {}
    for big in getBigPlays(team):
        if big[0] not in bigs:
            bigs[big[0]] = []
        bigs[big[0]].append(big)

    minYds = 15
    for cat, list in bigs.items():
        count = 0
        for play in list:
            if int(play[3]) > minYds:
                #print(play)
                count += 1

        #print('Category: {}, Number: {}'.format(cat, count))

def determineShortNames():
    cGame.execute('select id from game')
    games = []
    for row in cGame.fetchall():
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
            cGame.execute('update team set short_name="{}" where team_name="{}"'.format(shortName, otherTeam))

dates = getPlays('Nebraska')
for date, possessions in dates.items():
    print(date)
    for possession in possessions:
        print(str(possession))
            #for bigplay in getBigPlays('Nebraska'):
    #print(str(bigplay))
