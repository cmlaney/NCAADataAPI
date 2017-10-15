import json
import requests
from Game import Game, TeamInfo, TeamStats, Boxscore, Recap, Possession, Play, ScoringSummary, Score, PlayByPlay
import sqlite3
import re

conn = sqlite3.connect("games.sqlite")
c = conn.cursor()

def createDB():

    tn_team = 'team'

    c.execute('CREATE TABLE {} ({} {});'.format(tn_team, 'team_name', 'CHAR(100)'))

    tn_game = 'game'
    tn_game_cols_str = ['conference', 'start_date', 'location', \
                   'home_rank', 'home_iconURL', 'home_name', 'home_record', 'home_score', 'home_score_breakdown', \
                   'visiting_rank', 'visiting_iconURL', 'visiting_name', 'visiting_record', 'visiting_score', \
                   'visiting_score_breakdown', 'winner_name', 'recap_title', 'recap', 'game_url']

    c.execute('CREATE TABLE {} ({} {});'.format(tn_game, 'id', 'INTEGER PRIMARY KEY'))
    c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format(tn_game, 'start_time_epoch', 'INTEGER'))
    for col in tn_game_cols_str:
        c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format(tn_game, col, 'CHAR(1024)'))


    tn_stats = 'stats'

    c.execute('CREATE TABLE {} ({} {});'.format(tn_stats, 'id', 'INTEGER PRIMARY KEY'))
    c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format(tn_stats, 'game_id', 'INTEGER'))
    c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format(tn_stats, 'team_name', 'char(100)'))

    tn_stats_punt_ret_num = ['punt_ret_avg', 'punt_ret_yds', 'punt_ret_long', 'punt_ret_num']

    c.execute('CREATE TABLE {} ({} {});'.format('punt_return', 'stats_id', 'INTEGER'))
    c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format('punt_return', 'punt_ret_name', 'char(1024)'))
    for col in tn_stats_punt_ret_num:
        c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format('punt_return', col, 'REAL'))

    tn_stats_recv_num = ['recv_num', 'recv_td', 'recv_yds', 'recv_long']

    c.execute('CREATE TABLE {} ({} {});'.format('recv', 'stats_id', 'INTEGER'))
    c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format('recv', 'recv_name', 'char(1024)'))
    for col in tn_stats_recv_num:
        c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format('recv', col, 'REAL'))

    tn_stats_pass_num = ['pass_att', 'pass_comp', 'pass_int', 'pass_yds', 'pass_long', 'pass_td']

    c.execute('CREATE TABLE {} ({} {});'.format('pass', 'stats_id', 'INTEGER'))
    c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format('pass', 'pass_name', 'char(1024)'))
    for col in tn_stats_pass_num:
        c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format('pass', col, 'REAL'))

    tn_stats_rush_num = ['rush_num', 'rush_td', 'rush_yds', 'rush_long']

    c.execute('CREATE TABLE {} ({} {});'.format('rush', 'stats_id', 'INTEGER'))
    c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format('rush', 'rush_name', 'char(1024)'))
    for col in tn_stats_rush_num:
        c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format('rush', col, 'REAL'))

    tn_stats_kick_num = ['kick_fg', 'kick_fga', 'kick_xp', 'kick_pts', 'kick_long']

    c.execute('CREATE TABLE {} ({} {});'.format('kick', 'stats_id', 'INTEGER'))
    c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format('kick', 'kick_name', 'char(1024)'))
    for col in tn_stats_kick_num:
        c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format('kick', col, 'REAL'))

    tn_stats_punt_num = ['punt_avg', 'punt_long', 'punt_yds', 'punt_num']

    c.execute('CREATE TABLE {} ({} {});'.format('punt', 'stats_id', 'INTEGER'))
    c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format('punt', 'punt_name', 'char(1024)'))
    for col in tn_stats_punt_num:
        c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format('punt', col, 'REAL'))

    tn_stats_kick_ret_num = ['kick_ret_avg', 'kick_ret_yds', 'kick_ret_long', 'kick_ret_num']

    c.execute('CREATE TABLE {} ({} {});'.format('kick_ret', 'stats_id', 'INTEGER'))
    c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format('kick_ret', 'kick_ret_name', 'char(1024)'))
    for col in tn_stats_kick_ret_num:
        c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format('kick_ret', col, 'REAL'))

    tn_team_stats = 'team_stats'
    tn_team_stats_cols_num = ['stat_3rd_down_att', 'stat_3rd_down_conv', 'off_yds', 'avg_yds_per_play', 'off_plays', \
                              'stat_4th_down_att', 'stat_4th_down_conv', 'stat_1st_downs_total', \
                              'stat_1st_downs_from_penalty', 'stat_1st_down_pass', 'stat_1st_down_rush', \
                              'penalty_num', 'penalty_yards', 'int_recov_num', 'int_recov_yds', 'punt_ret_num', 'punt_ret_yds', \
                              'pass_att', 'pass_comp', 'pass_yds_total', 'pass_yds_avg', 'pass_int_thrown', 'kick_ret_num', \
                              'kick_ret_yds', 'rush_num', 'rush_yds_total', 'rush_yds_avg', 'fum_num', 'fum_lost', 'punt_num', \
                              'punt_yds']

    c.execute('CREATE TABLE {} ({} {});'.format(tn_team_stats, 'game_id', 'INTEGER'))
    c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format(tn_team_stats, 'team_name', 'char(100)'))
    for col in tn_team_stats_cols_num:
        c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format(tn_team_stats, col, 'REAL'))

    tn_possession = 'possession'
    tn_poss_cols_str = ['team_name', 'time', 'period']

    c.execute('CREATE TABLE {} ({} {});'.format(tn_possession, 'id', 'INTEGER PRIMARY KEY'))
    c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format(tn_possession, 'game_id', 'INTEGER'))
    for col in tn_poss_cols_str:
        c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format(tn_possession, col, 'CHAR(100)'))

    tn_play = 'play'
    tn_play_cols_str = ['text', 'drive']
    tn_play_cols_num = ['hScore', 'vScore']

    c.execute('CREATE TABLE {} ({} {});'.format(tn_play, 'possession_id', 'INTEGER'))
    for col in tn_play_cols_str:
        c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format(tn_play, col, 'CHAR(100)'))
    for col in tn_play_cols_num:
        c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format(tn_play, col, 'INTEGER'))

    tn_score = 'score'
    tn_score_cols_str = ['team_name', 'time', 'type', 'text', 'drive']
    tn_score_cols_num = ['vScore', 'hScore', 'period']

    c.execute('CREATE TABLE {} ({} {});'.format(tn_score, 'game_id', 'INTEGER'))
    for col in tn_score_cols_str:
        c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format(tn_score, col, 'CHAR(100)'))
    for col in tn_score_cols_num:
        c.execute("ALTER TABLE {} ADD COLUMN '{}' {}".format(tn_score, col, 'INTEGER'))

    c.execute("CREATE INDEX 'game_index' ON 'game' ('id' ASC);")
    c.execute("CREATE INDEX 'play_index' ON 'play' ('possession_id' ASC);")
    c.execute("CREATE INDEX 'poss_index' ON 'possession' ('game_id' ASC);")
    c.execute("CREATE INDEX 'score_index' ON 'score' ('game_id' ASC);")
    c.execute("CREATE INDEX 'stats_index' ON 'stats' ('game_id' ASC);")
    c.execute("CREATE INDEX 'team_stats_index' ON 'possession' ('game_id' ASC);")

def checkIfGameExists(url):
    c.execute('select * from game where game_url="{}"'.format(url))
    id_exists = c.fetchone()
    if id_exists:
        return True
    else:
        return False

def storeGame(game):

    recapTitle = ''
    recapText = ''
    if hasattr(game, 'recap'):
        recapTitle = game.recap.title
        recapText = game.recap.content


    c.execute('insert into game (start_time_epoch, conference, start_date, location, home_rank, home_iconURL, \
    home_name, home_record, home_score, home_score_breakdown, visiting_rank, visiting_iconURL, visiting_name, \
    visiting_record, visiting_score, visiting_score_breakdown, winner_name, recap_title, recap, game_url) values ({}, "{}", \
    "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}");'.format(\
        game.startTimeEpoch, game.conference, game.startDate, game.location, game.homeTeam.rank, game.homeTeam.iconURL, \
        game.homeTeam.nameRaw, game.homeTeam.record, game.homeTeam.currentScore, game.homeTeam.scoreBreakdown, \
        game.awayTeam.rank, game.awayTeam.iconURL, game.awayTeam.nameRaw, game.awayTeam.record, game.awayTeam.currentScore, \
        game.awayTeam.scoreBreakdown, game.winningTeam.nameRaw, recapTitle, recapText, game.url))
    last_game_id = c.lastrowid

    if hasattr(game, 'teamStats'):
        ts = game.teamStats
        for teamName, stats in ts.stats.items():
            stat_3rd_down = stats['Third-Down Conversions']['data'].strip().replace('-', ' ').split()
            stat_3rd_down_conv = float(stat_3rd_down[0])
            stat_3rd_down_att = float(stat_3rd_down[1])

            stat_off_yds = float(stats['Total Offense']['data'])
            stat_avg_yds_per_play = float(stats['Total Offense']['breakdown']['Avg. Per Play'].replace('-', '0'))
            stat_off_plays = int(stats['Total Offense']['breakdown']['Plays'])

            stat_4th_down = stats['Fourth-Down Conversions']['data'].strip().replace('-', ' ').split()
            stat_4th_down_conv = int(stat_4th_down[0])
            stat_4th_down_att = int(stat_4th_down[1])

            stat_1st_downs = int(stats['1st Downs']['data'])
            stat_1st_downs_pen = int(stats['1st Downs']['breakdown']['Penalty'])
            stat_1st_downs_pass = int(stats['1st Downs']['breakdown']['Passing'])
            stat_1st_downs_rush = int(stats['1st Downs']['breakdown']['Rushing'])

            stat_penalties = (stats['Penalties: Number-Yards']['data']).strip().replace('-', ' ').split()
            stat_pen_num = int(stat_penalties[0])
            stat_pen_yds = float(stat_penalties[1])

            stat_int_rets = (stats['Interception Returns: Number-Yards']['data']).strip().replace('-', ' ').split()
            stat_int_rets_num = int(stat_int_rets[0])
            stat_int_rets_yds = float(stat_int_rets[1])

            stat_punt_ret = (stats['Punt Returns: Number-Yards']['data']).strip().replace('-', ' ').split()
            stat_punt_ret_num = int(stat_punt_ret[0])
            stat_punt_ret_yds = float(stat_punt_ret[1])

            stat_passing_yds = float(stats['Passing']['data'])
            stat_passing_att = int(stats['Passing']['breakdown']['Attempts'])
            stat_passing_avg = float(stats['Passing']['breakdown']['Avg. Per Pass'].replace('-', '0'))
            stat_passing_int = int(stats['Passing']['breakdown']['Interceptions'])
            stat_passing_comp = int(stats['Passing']['breakdown']['Completions'])

            stat_rush_yds = float(stats['Rushing']['data'])
            stat_rush_att = int(stats['Rushing']['breakdown']['Attempts'])
            stat_rush_avg = float(stats['Rushing']['breakdown']['Avg. Per Rush'].replace('-', '0'))

            stat_kick_ret = (stats['Kickoff Returns: Number-Yards']['data']).strip().replace('-', ' ').split()
            stat_kick_ret_num = int(stat_kick_ret[0])
            stat_kick_ret_yds = float(stat_kick_ret[1])

            stat_fum = (stats['Fumbles: Number-Lost']['data']).strip().replace('-', ' ').split()
            stat_fum_num = int(stat_fum[0])
            stat_fum_lost = int(stat_fum[1])

            stat_punt = (stats['Punting: Number-Yards']['data']).strip().replace('-', ' ').split()
            stat_punt_num = 0
            stat_punt_yds = 0
            if len(stat_punt) == 2:
                stat_punt_num = int(stat_punt[0])
                stat_punt_yds = float(stat_punt[1])

            c.execute('insert into team_stats (game_id, team_name, stat_3rd_down_att, stat_3rd_down_conv, off_yds, avg_yds_per_play, \
            off_plays, stat_4th_down_att, stat_4th_down_conv, stat_1st_downs_total, stat_1st_downs_from_penalty, \
            stat_1st_down_pass, stat_1st_down_rush, \
            penalty_num, penalty_yards, int_recov_num, int_recov_yds, punt_ret_num, punt_ret_yds, pass_att, pass_comp, \
            pass_yds_total, pass_yds_avg, pass_int_thrown, kick_ret_num, kick_ret_yds, rush_num, rush_yds_total, rush_yds_avg, \
            fum_num, fum_lost, punt_num, punt_yds) values ({}, "{}", {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, \
            {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})'.format(last_game_id, teamName, stat_3rd_down_att, \
                                                        stat_3rd_down_conv, stat_off_yds, stat_avg_yds_per_play, stat_off_plays, \
                                                                                        stat_4th_down_att, stat_4th_down_conv, \
                                                        stat_1st_downs, stat_1st_downs_pen, stat_1st_downs_pass, stat_1st_downs_rush, \
                                                        stat_pen_num, stat_pen_yds, stat_int_rets_num, stat_int_rets_yds, \
                                                        stat_punt_ret_num, stat_punt_ret_yds, stat_passing_att, stat_passing_comp, \
                                                        stat_passing_yds, stat_passing_avg, stat_passing_int, stat_kick_ret_num, \
                                                        stat_kick_ret_yds, stat_rush_att, stat_rush_yds, stat_rush_avg, stat_fum_num, \
                                                        stat_fum_lost, stat_punt_num, stat_punt_yds))

    if hasattr(game, 'boxscore'):
        team_name_visit = game.awayTeam.nameRaw
        c.execute('insert into stats (game_id, team_name) values ({}, "{}")'.format(last_game_id, team_name_visit))
        stats_id =  c.lastrowid
        punt_ret_visit = game.boxscore.tables['punt_returns_visiting']
        for name, punt_ret in punt_ret_visit.items():
            punt_ret['AVG'] = punt_ret['AVG'].replace('-', '0')
            if len(punt_ret['LONG']) == 0:
                punt_ret['LONG'] = '0'
            c.execute('insert into punt_return (stats_id, punt_ret_name, punt_ret_avg, punt_ret_yds, punt_ret_long, \
            punt_ret_num) values ({}, "{}", {}, {}, {}, {})'.format(stats_id, punt_ret['PUNT RETURNS'], \
                    float(punt_ret['AVG']), float(punt_ret['YDS']), float(punt_ret['LONG']), float(punt_ret['NO'])))

        passing_visit = game.boxscore.tables['passing_visiting']
        for name, passing in passing_visit.items():
            pass_att = passing['CP-ATT-INT'].strip().split('-')
            if len(passing['LONG']) == 0:
                passing['LONG'] = '0'
            c.execute('insert into pass (stats_id, pass_name, pass_att, pass_comp, pass_int, pass_yds, pass_td, \
            pass_long) values ({}, "{}", {}, {}, {}, {}, {}, {})'.format(stats_id, passing['PASSING'], float(pass_att[1]), \
                    float(pass_att[0]), float(pass_att[2]), float(passing['YDS']), float(passing['TD']), float(passing['LONG'])))

        receiving_visit = game.boxscore.tables['receiving_visiting']
        for name, recv in receiving_visit.items():
            if len(recv['LONG']) == 0:
                recv['LONG'] = '0'
            c.execute('insert into recv (stats_id, recv_name, recv_num, recv_td, recv_yds, recv_long) \
            values ({}, "{}", {}, {}, {}, {})'.format(stats_id, recv['RECEIVING'], float(recv['REC']), \
                    float(recv['TD']), float(recv['YDS']), float(recv['LONG'])))

        kicking_visit = game.boxscore.tables['kicking_visiting']
        for name, kick in kicking_visit.items():
            if len(kick['LONG'].replace('-', '')) == 0:
                kick['LONG'] = '0'
            fg_stat = kick['FG-FGA'].strip().split('/')
            c.execute('insert into kick (stats_id, kick_name, kick_fg, kick_fga, kick_xp, kick_pts, kick_long) \
            values ({}, "{}", {}, {}, {}, {}, {})'.format(stats_id, kick['KICKING'], float(fg_stat[0]), \
                        float(fg_stat[1]), float(kick['XP']), float(kick['PTS']), float(kick['LONG'])))

        punting_visit = game.boxscore.tables['punting_visiting']
        for name, punt in punting_visit.items():
            punt['AVG'] = punt['AVG'].replace('-', '0')
            punt['LONG'] = punt['LONG'].replace('-', '0')
            if len(punt['LONG']) == 0:
                punt['LONG'] = '0'
            if len(punt['AVG']) == 0:
                punt['AVG'] = '0'
            if len(punt['YDS']) == 0:
                punt['YDS'] = '0'
            if len(punt['NO']) == 0:
                punt['NO'] = '0'
            if len(punt['LONG'].replace('-', '')) == 0:
                punt['LONG'] = '0'
            if len(punt['AVG'].replace('-', '')) == 0:
                punt['AVG'] = '0'
            c.execute('insert into punt (stats_id, punt_name, punt_avg, punt_long, punt_yds, punt_num) \
            values ({}, "{}", {}, {}, {}, {})'.format(stats_id, punt['PUNTING'], float(punt['AVG']), \
                                                      float(punt['LONG']), float(punt['YDS']), float(punt['NO'])))

        rushing_visit = game.boxscore.tables['rushing_visiting']
        for name, rush in rushing_visit.items():
            if len(rush['LONG']) == 0:
                rush['LONG'] = '0'
            c.execute('insert into rush (stats_id, rush_name, rush_num, rush_td, rush_yds, rush_long) \
            values ({}, "{}", {}, {}, {}, {})'.format(stats_id, rush['RUSHING'], float(rush['ATT']), \
                                                      float(rush['TD']), float(rush['YDS']), float(rush['LONG'])))

        kick_returns_visit = game.boxscore.tables['kick_returns_visiting']
        for name, kick_ret in kick_returns_visit.items():
            kick_ret['AVG'] = kick_ret['AVG'].replace('-', '0')
            kick_ret['LONG'] = kick_ret['LONG'].replace('-', '0')
            if len(kick_ret['LONG']) == 0:
                kick_ret['LONG'] = '0'
            if len(kick_ret['AVG']) == 0:
                kick_ret['AVG'] = '0'
            if len(kick_ret['YDS']) == 0:
                kick_ret['YDS'] = '0'
            if len(kick_ret['NO']) == 0:
                kick_ret['NO'] = '0'
            c.execute('insert into kick_ret (stats_id, kick_ret_name, kick_ret_avg, kick_ret_yds, kick_ret_long, \
            kick_ret_num) values ({}, "{}", {}, {}, {}, {})'.format(stats_id, kick_ret['KICK RETURNS'], \
                    float(kick_ret['AVG']), float(kick_ret['YDS']), float(kick_ret['LONG']), float(kick_ret['NO'])))

        team_name_home = game.homeTeam.nameRaw
        c.execute('insert into stats (game_id, team_name) values ({}, "{}")'.format(last_game_id, team_name_home))
        stats_id = c.lastrowid
        punt_ret_home = game.boxscore.tables['punt_returns_home']
        for name, punt_ret in punt_ret_home.items():
            punt_ret['AVG'] = punt_ret['AVG'].replace('-', '0')
            if len(punt_ret['LONG']) == 0:
                punt_ret['LONG'] = '0'
            c.execute('insert into punt_return (stats_id, punt_ret_name, punt_ret_avg, punt_ret_yds, punt_ret_long, \
                    punt_ret_num) values ({}, "{}", {}, {}, {}, {})'.format(stats_id, punt_ret['PUNT RETURNS'], \
                                                                             float(punt_ret['AVG']),
                                                                             float(punt_ret['YDS']),
                                                                             float(punt_ret['LONG']),
                                                                             float(punt_ret['NO'])))

        passing_home = game.boxscore.tables['passing_home']
        for name, passing in passing_home.items():
            pass_att = passing['CP-ATT-INT'].strip().split('-')
            if len(passing['LONG']) == 0:
                passing['LONG'] = '0'
            c.execute('insert into pass (stats_id, pass_name, pass_att, pass_comp, pass_int, pass_yds, pass_td, \
                    pass_long) values ({}, "{}", {}, {}, {}, {}, {}, {})'.format(stats_id, passing['PASSING'],
                                                                                  float(pass_att[1]), \
                                                                                  float(pass_att[0]),
                                                                                  float(pass_att[2]),
                                                                                  float(passing['YDS']),
                                                                                  float(passing['TD']),
                                                                                  float(passing['LONG'])))

        receiving_home = game.boxscore.tables['receiving_home']
        for name, recv in receiving_home.items():
            if len(recv['LONG']) == 0:
                recv['LONG'] = '0'
            c.execute('insert into recv (stats_id, recv_name, recv_num, recv_td, recv_yds, recv_long) \
                    values ({}, "{}", {}, {}, {}, {})'.format(stats_id, recv['RECEIVING'], float(recv['REC']), \
                                                              float(recv['TD']), float(recv['YDS']),
                                                              float(recv['LONG'])))

        kicking_home = game.boxscore.tables['kicking_home']
        for name, kick in kicking_home.items():
            if len(kick['LONG'].replace('-', '')) == 0:
                kick['LONG'] = '0'
            if len(kick['PTS'].replace('-', '')) == 0:
                kick['PTS'] = '0'
            fg_stat = kick['FG-FGA'].strip().split('/')
            c.execute('insert into kick (stats_id, kick_name, kick_fg, kick_fga, kick_xp, kick_pts, kick_long) \
                    values ({}, "{}", {}, {}, {}, {}, {})'.format(stats_id, kick['KICKING'], float(fg_stat[0]), \
                                                                  float(fg_stat[1]), float(kick['XP']),
                                                                  float(kick['PTS']), float(kick['LONG'])))

        punting_home = game.boxscore.tables['punting_home']
        for name, punt in punting_home.items():
            punt['AVG'] = punt['AVG'].replace('-', '0')
            punt['LONG'] = punt['LONG'].replace('-', '0')
            if len(punt['LONG']) == 0:
                punt['LONG'] = '0'
            if len(punt['AVG']) == 0:
                punt['AVG'] = '0'
            if len(punt['YDS']) == 0:
                punt['YDS'] = '0'
            if len(punt['NO']) == 0:
                punt['NO'] = '0'
            c.execute('insert into punt (stats_id, punt_name, punt_avg, punt_long, punt_yds, punt_num) \
                    values ({}, "{}", {}, {}, {}, {})'.format(stats_id, punt['PUNTING'], float(punt['AVG']), \
                                                              float(punt['LONG']), float(punt['YDS']),
                                                              float(punt['NO'])))

        rushing_home = game.boxscore.tables['rushing_home']
        for name, rush in rushing_home.items():
            if len(rush['LONG']) == 0:
                rush['LONG'] = '0'
            c.execute('insert into rush (stats_id, rush_name, rush_num, rush_td, rush_yds, rush_long) \
                    values ({}, "{}", {}, {}, {}, {})'.format(stats_id, rush['RUSHING'], float(rush['ATT']), \
                                                              float(rush['TD']), float(rush['YDS']),
                                                              float(rush['LONG'])))

        kick_returns_home = game.boxscore.tables['kick_returns_home']
        for name, kick_ret in kick_returns_home.items():
            kick_ret['AVG'] = kick_ret['AVG'].replace('-', '0')
            kick_ret['LONG'] = kick_ret['LONG'].replace('-', '0')
            if len(kick_ret['LONG']) == 0:
                kick_ret['LONG'] = '0'
            if len(kick_ret['AVG']) == 0:
                kick_ret['AVG'] = '0'
            if len(kick_ret['YDS']) == 0:
                kick_ret['YDS'] = '0'
            if len(kick_ret['NO']) == 0:
                kick_ret['NO'] = '0'
            c.execute('insert into kick_ret (stats_id, kick_ret_name, kick_ret_avg, kick_ret_yds, kick_ret_long, \
                    kick_ret_num) values ({}, "{}", {}, {}, {}, {})'.format(stats_id, kick_ret['KICK RETURNS'], \
                                                                             float(kick_ret['AVG'].replace('-', '0')),
                                                                             float(kick_ret['YDS']),
                                                                             float(kick_ret['LONG']),
                                                                             float(kick_ret['NO'])))

    if hasattr(game, 'scoringSummary'):
        for period, scores in game.scoringSummary.periods.items():
            period_int = getPeriodInt(period)
            for score in scores:
                c.execute('insert into score (game_id, team_name, period, time, type, text, drive, vScore, hScore) \
                values ({}, "{}", {}, "{}", "{}", "{}", "{}", {}, {})'.format(last_game_id, score.teamName, period_int, \
                                                                                score.time, score.type, score.text, score.drive, \
                                                                                int(score.visitorScore), int(score.homeScore)))

    if hasattr(game, 'playByPlay'):
        for period, possessions in game.playByPlay.periods.items():
            period_int = getPeriodInt(period)
            for possession in possessions:
                c.execute('insert into possession (game_id, team_name, time, period) values ({}, "{}", "{}", {})'\
                          .format(last_game_id, possession.teamName, possession.time, period_int))
                possession_id = c.lastrowid
                for play in possession.plays:
                    c.execute('insert into play (possession_id, text, drive, hScore, vScore) values ({}, "{}", "{}", {}, {})'\
                              .format(possession_id, play.text, play.drive, play.hScore, play.vScore))
                    play.id = c.lastrowid
                    profilePlay(play, possession.teamName)
                    updatePlay(play)

def profilePlay(play, team):
    c.execute('select short_name from team where team_name="{}"'.format(team))
    result = c.fetchone()
    if result is not None and len(result) > 0:
        shortName = result[0]
    else:
        return
    intercepted = False

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

def getPeriodInt(period):
    period_int = -1
    if 'OT' in period:
        period_int = 5
    elif '2OT' in period:
        period_int = 6
    elif '3OT' in period:
        period_int = 7
    elif '4OT' in period:
        period_int = 8
    elif '5OT' in period:
        period_int = 9
    elif '6OT' in period:
        period_int = 10
    elif '7OT' in period:
        period_int = 11
    elif '8OT' in period:
        period_int = 12
    elif '9OT' in period:
        period_int = 13
    elif '10OT' in period:
        period_int = 14
    elif '1' in period:
        period_int = 1
    elif '2' in period:
        period_int = 2
    elif '3' in period:
        period_int = 3
    elif '4' in period:
        period_int = 4
    return period_int

def fetchGames(week, refetch=False, store=False):
    url = 'http://data.ncaa.com/sites/default/files/data/scoreboard/football/fbs/2017/{}/scoreboard.json'.format(week)
    data = requests.get(url=url)
    output = json.loads(data.content)
    games = {}

    for group in output['scoreboard']:
        print(group['day'])
        for game in group['games']:
            key, newGame = fetchSpecific(game, refetch)
            if len(key) > 0:
                games[key] = newGame
                if store:
                    print('Storing: {}'.format(key))
                    if newGame.finalMessage == 'Final':
                        storeGame(newGame)
                    conn.commit()
    return games

def fetchSpecific(game, refetch = False):
    exists = checkIfGameExists(game)
    if not exists or refetch:
        gameUrl = 'http://data.ncaa.com' + str(game)
        gameResp = requests.get(url=gameUrl)
        gameData = json.loads(gameResp.content)
        if 'gameStatus' not in gameData:
            gameData['gameStatus'] = ''
        if 'periodStatus' not in gameData:
            gameData['periodStatus'] = ''
        if 'downToGo' not in gameData:
            gameData['downToGo'] = ''
        if 'finalMessage' not in gameData:
            gameData['finalMessage'] = ''
        if 'currentPeriod' not in gameData:
            gameData['currentPeriod'] = ''
        newGame = Game(gameData['id'], gameData['conference'], gameData['updatedTimestamp'], gameData['gameState'], \
                       gameData['startDate'], gameData['startTimeEpoch'], gameData['currentPeriod'], \
                       gameData['finalMessage'], gameData['gameStatus'], gameData['periodStatus'], \
                       gameData['downToGo'], gameData['timeclock'], gameData['location'], \
                       gameData['scoreBreakdown'], gameData['home'], gameData['away'], gameData['tabsArray'], game)
        key = 'Home: {} - Away: {} - Date: {}'.format(newGame.homeTeam.nameRaw, newGame.awayTeam.nameRaw,
                                                      newGame.startDate)
        if hasattr(newGame, 'playByPlay'):
            updatePlayScores(newGame.playByPlay)
        print('Fetched: {}'.format(key))
        return key, newGame
    else:
        print('Game exists in DB and refetch not specified: {}'.format(game))
        return '', False

def updatePlayScores(playByPlay):
    periods = {}
    pbpPeriods = {}
    for period, possessions in playByPlay.periods.items():
        pbpPeriods[getPeriodInt(period)] = possessions
    for period, possessions in pbpPeriods.items():
        for possession in possessions:
            if period not in periods:
                periods[period] = {}
            time = possession.time
            if time[0] == ':':
                time = '00'+time
            if time in periods[period]:
                time += ' '
            periods[period][time] = possession

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

def populate(weeks):
    for week in weeks:
        games = fetchGames(week=week, store=True)
        for key, game in games.items():
            print('Storing: {}'.format(key))
            if game.finalMessage == 'Final':
                storeGame(game)
    conn.commit()

#createDB()

#key, single_game = fetchSpecific('/sites/default/files/data/game/football/fbs/2017/09/02/florida-st-alabama/gameinfo.json', refetch=True)
#print(single_game.playByPlay)
#if len(key) > 0:
    #storeGame(single_game)
#conn.commit()

weeks = ['07']
populate(weeks)


conn.close()

