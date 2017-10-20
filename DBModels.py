from sqlalchemy import Column, ForeignKey, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Team(Base):
    __tablename__ = 'team'

    rowid = Column(Integer, primary_key=True)
    team_name = Column(String(255))
    conference = Column(String(255))
    division = Column(String(255))
    short_name = Column(String(255))

    def __str__(self):
        return '{} : {} - {} : {}'.format(self.team_name, self.conference, self.division, self.short_name)

class Game(Base):
    __tablename__ = 'game'

    id = Column(Integer, primary_key=True)
    start_time_epoch = Column(Integer)
    conference = Column(String(255))
    start_date = Column(String(255))
    location = Column(String(512))
    home_rank = Column(String(255))
    home_iconURL = Column(String(1000))
    home_name = Column(String(255))
    home_record = Column(String(255))
    home_score = Column(String(255))
    home_score_breakdown = Column(String(255))
    visiting_rank = Column(String(255))
    visiting_iconURL = Column(String(1000))
    visiting_name = Column(String(255))
    visiting_record = Column(String(255))
    visiting_score = Column(String(255))
    visiting_score_breakdown = Column(String(255))
    winner_name = Column(String(255))
    recap_title = Column(String(512))
    recap = Column(String(4000))
    game_url = Column(String(1000))

    def __str__(self):
        return '{} - {}, {} - {}'.format(self.home_name, self.home_score, self.visiting_name, self.visiting_score)

class Score(Base):
    __tablename__ = 'score'

    rowid = Column(Integer, primary_key=True)
    game_id = Column(Integer, ForeignKey('game.id'))
    game = relationship(Game)
    team_name = Column(String(255))
    time = Column(String(255))
    type = Column(String(100))
    text = Column(String(1000))
    drive = Column(String(1000))
    vScore = Column(Integer)
    hScore = Column(Integer)
    period = Column(Integer)

    def __str__(self):
        return '{} vs {}, {}\n{}, {}\n{}\nHome: {}, Visitor: {}'.format(self.game.home_name, self.game.visiting_name, \
                                self.game.start_date, self.type, self.team_name, self.text, self.hScore, self.vScore)

class Stats(Base):
    __tablename__ = 'stats'

    id = Column(Integer, primary_key=True)
    game_id = Column(Integer, ForeignKey('game.id'))
    game = relationship(Game)
    team_name = Column(String(255))

class Kick(Base):
    __tablename__ = 'kick'

    rowid = Column(Integer, primary_key=True)
    stats_id = Column(Integer, ForeignKey('stats.id'))
    stats = relationship(Stats)
    kick_name = Column(String(255))
    kick_fg = Column(Float)
    kick_fga = Column(Float)
    kick_xp = Column(Float)
    kick_pts = Column(Float)
    kick_long = Column(Float)

    def __str__(self):
        return '{}\nField Goals Att: {}\nField Goals Made: {}\nField Goal Long: {}\nExtra Points Made: {}\nPoints: {}\n'\
    .format(self.kick_name, self.kick_fga, self.kick_fg, self.kick_long, self.kick_xp, self.kick_pts)

class KickRet(Base):
    __tablename__ = 'kick_ret'

    rowid = Column(Integer, primary_key=True)
    stats_id = Column(Integer, ForeignKey('stats.id'))
    stats = relationship(Stats)
    kick_ret_name = Column(String(255))
    kick_ret_avg = Column(Float)
    kick_ret_yds = Column(Float)
    kick_ret_long = Column(Float)
    kick_ret_num = Column(Float)

    def __str__(self):
        return '{}\nReturn Avg: {}\nReturn Yards: {}\nLongest Return: {}\nNo. Returns: {}\n'\
    .format(self.kick_ret_name, self.kick_ret_avg, self.kick_ret_yds, self.kick_ret_long, self.kick_ret_num)

class Pass(Base):
    __tablename__ = 'pass'

    rowid = Column(Integer, primary_key=True)
    stats_id = Column(Integer, ForeignKey('stats.id'))
    stats = relationship(Stats)
    pass_name = Column(String(255))
    pass_att = Column(Float)
    pass_comp = Column(Float)
    pass_int = Column(Float)
    pass_yds = Column(Float)
    pass_long = Column(Float)
    pass_td = Column(Float)

    def __str__(self):
        return '{}\nAttempts: {}\nCompletions: {}\nInterceptions: {}\nYards: {}\nLongest Pass: {}\nTDs: {}\n'\
    .format(self.pass_name, self.pass_att, self.pass_comp, self.pass_int, self.pass_yds, self.pass_long, self.pass_td)

class Recv(Base):
    __tablename__ = 'recv'

    rowid = Column(Integer, primary_key=True)
    stats_id = Column(Integer, ForeignKey('stats.id'))
    stats = relationship(Stats)
    recv_name = Column(String(255))
    recv_num = Column(Float)
    recv_td = Column(Float)
    recv_yds = Column(Float)
    recv_long = Column(Float)

    def __str__(self):
        return '{}\nNo. Receptions: {}\nTDs: {}\nYards: {}\nLongest Reception: {}\n'\
    .format(self.recv_name, self.recv_num, self.recv_td, self.recv_yds, self.recv_long)

class Rush(Base):
    __tablename__ = 'rush'

    rowid = Column(Integer, primary_key=True)
    stats_id = Column(Integer, ForeignKey('stats.id'))
    stats = relationship(Stats)
    rush_name = Column(String(255))
    rush_num = Column(Float)
    rush_td = Column(Float)
    rush_yds = Column(Float)
    rush_long = Column(Float)

    def __str__(self):
        return '{}\nNo. Attempts: {}\nTDs: {}\nYards: {}\nLongest Rush: {}\n'\
    .format(self.rush_name, self.rush_num, self.rush_td, self.rush_yds, self.rush_long)

class Punt(Base):
    __tablename__ = 'punt'

    rowid = Column(Integer, primary_key=True)
    stats_id = Column(Integer, ForeignKey('stats.id'))
    stats = relationship(Stats)
    punt_name = Column(String(255))
    punt_avg = Column(Float)
    punt_long = Column(Float)
    punt_yds = Column(Float)
    punt_num = Column(Float)

    def __str__(self):
        return '{}\nPunt Avg: {}\nLongest Punt: {}\nPunt Yards: {}\nNo. Punts: {}\n'\
    .format(self.punt_name, self.punt_avg, self.punt_long, self.punt_yds, self.punt_num)

class PuntRet(Base):
    __tablename__ = 'punt_return'

    rowid = Column(Integer, primary_key=True)
    stats_id = Column(Integer, ForeignKey('stats.id'))
    stats = relationship(Stats)
    punt_ret_name = Column(String(255))
    punt_ret_avg = Column(Float)
    punt_ret_yds = Column(Float)
    punt_ret_long = Column(Float)
    punt_ret_num = Column(Float)

    def __str__(self):
        return '{}\nReturn Avg: {}\nReturn Yards: {}\nLongest Return: {}\nNo. Returns: {}\n'\
    .format(self.punt_ret_name, self.punt_ret_avg, self.punt_ret_yds, self.punt_ret_long, self.punt_ret_num)

class TeamStats(Base):
    __tablename__ = 'team_stats'

    rowid = Column(Integer, primary_key=True)
    game_id = Column(Integer, ForeignKey('game.id'))
    game = relationship(Game)
    team_name = Column(String(255))
    stat_3rd_down_att = Column(Float)
    stat_3rd_down_conv = Column(Float)
    off_yds = Column(Float)
    avg_yds_per_play = Column(Float)
    off_plays = Column(Float)
    stat_4th_down_att = Column(Float)
    stat_4th_down_conv = Column(Float)
    stat_1st_downs_total = Column(Float)
    stat_1st_downs_from_penalty = Column(Float)
    stat_1st_down_pass = Column(Float)
    stat_1st_down_rush = Column(Float)
    penalty_num = Column(Float)
    penalty_yards = Column(Float)
    int_recov_num = Column(Float)
    int_recov_yds = Column(Float)
    punt_ret_num = Column(Float)
    punt_ret_yds = Column(Float)
    pass_att = Column(Float)
    pass_comp = Column(Float)
    pass_yds_total = Column(Float)
    pass_yds_avg = Column(Float)
    pass_int_thrown = Column(Float)
    kick_ret_num = Column(Float)
    kick_ret_yds = Column(Float)
    rush_num = Column(Float)
    rush_yds_total = Column(Float)
    rush_yds_avg = Column(Float)
    fum_num = Column(Float)
    fum_lost = Column(Float)
    punt_num = Column(Float)
    punt_yds = Column(Float)

    def __str__(self):
        output = '{} - {}\n'.format(self.team_name, self.game.start_date)
        attrs = [a for a in dir(self) if not a.startswith('_') and a not in ['team_name', 'game', 'game_id', 'rowid', 'metadata']]
        for attr in attrs:
            output += '{}: {}\n'.format(attr, self.__dict__[attr])
        return output

class Possession(Base):
        __tablename__ = 'possession'

        id = Column(Integer, primary_key=True)
        game_id = Column(Integer, ForeignKey('game.id'))
        game = relationship(Game)
        team_name = Column(String(255), ForeignKey('team.team_name'))
        team = relationship(Team)
        time = Column(String(255))
        period = Column(Integer)

        def __str__(self):
            return '{}, {}\n{} - {}'.format(self.team_name, self.game.start_date, self.period, self.time)

class Play(Base):
        __tablename__ = 'play'

        rowid = Column(Integer, primary_key=True)
        possession_id = Column(Integer, ForeignKey('possession.id'))
        possession = relationship(Possession)
        text = Column(String(255))
        drive = Column(String(255))
        vScore = Column(Integer)
        hScore = Column(Integer)
        intercepted = Column(Integer)
        fumbled = Column(Integer)
        touchdown = Column(Integer)
        passing = Column(Integer)
        rushing = Column(Integer)
        completion = Column(Integer)
        punt = Column(Integer)
        punt_yds = Column(Integer)
        fair_catch = Column(Integer)
        downed = Column(Integer)
        kick_ret = Column(Integer)
        kick_ret_yds = Column(Integer)
        field_pos = Column(Integer)
        touchback = Column(Integer)
        yds = Column(Integer)
        extra_point = Column(Integer)
        xp_good = Column(Integer)
        sacked = Column(Integer)
        field_goal = Column(Integer)
        field_goal_yds = Column(Integer)
        safety = Column(Integer)

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


