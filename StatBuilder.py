
import sqlite3
from DBModels import Base, Game, Team, Play, Possession, TeamStats, Stats, Kick, KickRet, Pass, Recv, Rush, Punt, \
    PuntRet, Score
import re
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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

def getFilteredPassingPlays(team, yds=0, pos=0, int=0, completion=1, margin=0, fumbled=0):
    cGame.execute('select text, drive, hScore, vScore, intercepted, fumbled, touchdown, completion, field_pos, yds, home '
                  'from play join possession as poss on play.possession_id=poss.id where team_name="{}" and passing=1'.format(team))
    plays = {}
    plays['home'] = []
    plays['away'] = []
    for row in cGame.fetchall():
        newPlay = Play(row[0], row[1], row[3], row[2], -1)
        newPlay.passing = 1
        newPlay.intercepted = row[4]
        newPlay.fumbled = row[5]
        newPlay.touchdown = row[6]
        newPlay.completion = row[7]
        newPlay.field_pos = row[8]
        newPlay.yds = row[9]
        if newPlay.yds >= yds and newPlay.field_pos >= pos and newPlay.intercepted == int and newPlay.completion == completion and newPlay.fumbled == fumbled:
            if (row[10] == 0 and newPlay.vScore-newPlay.hScore >= margin) or (row[10] == 1 and newPlay.hScore-newPlay.vScore >= margin):
                if row[10] > 0:
                    plays['home'].append(newPlay)
                else:
                    plays['away'].append(newPlay)
    return plays

def getFilteredRushingPlays(team, yds=0, pos=0, margin=0, fumbled=0):
    cGame.execute('select text, drive, hScore, vScore, fumbled, touchdown, field_pos, yds, home '
                  'from play join possession as poss on play.possession_id=poss.id where team_name="{}" and rushing=1'.format(team))
    plays = {}
    plays['home'] = []
    plays['away'] = []
    for row in cGame.fetchall():
        newPlay = Play(row[0], row[1], row[3], row[2], -1)
        newPlay.rushing = 1
        newPlay.fumbled = row[4]
        newPlay.touchdown = row[5]
        newPlay.field_pos = row[6]
        newPlay.yds = row[7]
        home = row[8]
        if newPlay.yds >= yds and newPlay.field_pos >= pos and newPlay.fumbled == fumbled:
            if (home == 0 and newPlay.vScore-newPlay.hScore >= margin) or (home == 1 and newPlay.hScore-newPlay.vScore >= margin):
                if home > 0:
                    plays['home'].append(newPlay)
                else:
                    plays['away'].append(newPlay)



engine = create_engine('sqlite:///games.sqlite')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

rows = session.query(Play).filter(Play.passing == 1).filter(Play.completion == 1).filter(Possession.team_name == 'Nebraska').all()
#rows = session.query(Possession).all()
for row in rows:
    print(row)

connGame.close()
