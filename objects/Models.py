class Possession(object):

    def __init__(self, teamName, time, plays, period, id=-1):
        self.id = id
        self.teamName = teamName
        self.time = time
        self.plays = plays
        self.period = period


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
            self.text = text.encode('ascii')
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
        self.punt_yds = False
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

    def __str__(self):
        breakdown = ''
        if self.passing:
            breakdown = 'Passing: '
            if self.completion:
                breakdown += 'complete, for {} yards, {} to go'.format(self.yds, self.field_pos)
                if self.touchdown:
                    breakdown += ', touchdown'
            elif self.intercepted:
                breakdown += 'intercepted'
            elif self.fumbled:
                breakdown += 'fumbled'
            else:
                breakdown += 'incomplete'
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