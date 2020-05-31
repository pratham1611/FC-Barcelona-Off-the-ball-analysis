import os
import json
import pandas as pd
from pandas.io.json import json_normalize
import sqlalchemy
import pymysql
import numpy as np


def connection_handler(data,table):
	user='root'
	pwd='mysql'
	ip='127.0.0.1'
	name='laliga'
	conn=sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(user,pwd,ip,name))
	data.to_sql(con=conn, name=table, if_exists='append',index = False, chunksize=100000)

def load_competitions(filename):
	with open(filename) as data_file:
		data = json.load(data_file)
		df = pd.io.json.json_normalize(data)
		df = df[['competition_id','season_id','competition_name','season_name']]
		connection_handler(df,'competitions')

def load_games(filename):
	with open(filename,encoding="utf8") as data_file:
		data = json.load(data_file)
		df = pd.io.json.json_normalize(data)
		df['match_date'] = pd.to_datetime(df['match_date'], format='%Y-%m-%d')
		df.rename(columns={'competition.competition_id': 'competition_id', 
			'season.season_id': 'season_id','home_team.home_team_name':'home_team_name',
			'away_team.away_team_name':'away_team_name'}, inplace=True)
		df = df[['match_id','match_date','kick_off','competition_id','season_id',
		'home_team_name','away_team_name','home_score','away_score']]
		connection_handler(df,'games')
		return df['match_id'].tolist()



def load_plays(filename,game_id):
	with open(filename,encoding="utf8") as data_file:
		data = json.load(data_file)
		df = pd.io.json.json_normalize(data)
		df.insert(0, 'match_id',0)
		df = df.assign(match_id=game_id)
		df['recovery_outcome']='True'
		df['recovery_outcome'] = np.where((df['type.name']!='Ball Recovery'),'',df['recovery_outcome'])
		if 'ball_recovery.recovery_failure' in df.columns:
			df['recovery_outcome'] = np.where((df['ball_recovery.recovery_failure'].notnull()), 'False',df['recovery_outcome'])
		df.rename(columns={'index':'play_id','type.name':'play_type',
			'possession_team.name':'possession_team',
			'play_pattern.name':'play_pattern','player.name':'player_name','team.name':'team_name'},inplace=True)
		df_loc=df.location.apply(pd.Series)
		df_loc.columns=['location_x','location_y']
		df=pd.concat([df.drop(['location'], axis=1), df_loc], axis=1)
		df=df[['match_id','play_id','period','minute','second','play_type','play_pattern',
		'possession_team','player_name','team_name','location_x','location_y','recovery_outcome']]

		connection_handler(df,'plays')


def main():
	load_competitions('data/competitions.json')
	seasons=os.listdir('data/matches/11')
	games=[]
	for season in seasons:
		print(season)
		games=load_games('data/matches/11/'+season)

		for game in games:
			print(game)
			load_plays('data/events/'+str(game)+'.json',game)
	print("Done")



if __name__=='__main__':
	main()


