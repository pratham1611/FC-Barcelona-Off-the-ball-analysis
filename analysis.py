import mysql.connector
import pandas as pd
import sqlalchemy

SEASON_SQL='select season_id from competitions where competition_name="La Liga"'
GAMES_SQL='select match_id from games where season_id=%d'
PLAY_SQL='select a.* FROM (select * from plays where match_id=%(0)d)a '\
'WHERE a.possession_team <>  ( SELECT b.possession_team FROM '\
'(select * from plays where match_id=%(0)d)b WHERE a.match_id = b.match_id'\
' AND a.play_id > b.play_id ORDER BY b.play_id DESC LIMIT 1)'

TEAMS_URL='select DISTINCT team '\
'FROM ('\
 'select home_team_name AS team from games where season_id=%(0)d'\
    ' UNION select away_team_name AS team'\
    ' FROM games where season_id=%(0)d'\
') teams'\

RECOVERY_PLAYERS_SQL='select match_id,player_name, count(*) as recoveries from plays '\
'where match_id=%d and play_type="Ball Recovery" and team_name="Barcelona" and recovery_outcome="True"'\
'group by player_name'

RECOVERY_LOCATIONS_SQL='select match_id,play_id,team_name,player_name,location_x,location_y from plays '\
'where match_id=%d and play_type="Ball Recovery" and recovery_outcome="True"'\



hostname='127.0.0.1'
dbname='laliga'
username='root'
pwd='mysql'



def insert_df_to_sql(dataframe,table,hostname='127.0.0.1',dbname='laliga',username='root',pwd='mysql'):
	conn=sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(username,pwd,hostname,dbname))
	dataframe.to_sql(con=conn, name=table, if_exists='append',index = False)


def get_data(query,hostname='127.0.0.1',dbname='laliga',username='root',pwd='mysql',arraysize=4000):
	db = mysql.connector.connect(host=hostname,database=dbname,user=username,password=pwd)
	cursor = db.cursor(dictionary=True)
	cursor.execute(query)
	results = cursor.fetchmany(arraysize)
	cursor.close()
	db.close()
	return results


def extract_time_off_the_ball(query,season_id,match_id):
	df=pd.DataFrame(get_data(query),columns=['minute','second','possession_team'])
	df['time']=df['minute']*60+df['second']
	df['val']=df['time'].diff()
	df.val.fillna(df.time, inplace=True)

	time_avg_recovery=df.groupby(['possession_team'])[['val']].mean().reset_index()
	decimals = 2    
	time_avg_recovery['val'] = time_avg_recovery['val'].apply(lambda x: round(x, decimals))
	time_avg_recovery.insert(0, 'season_id',season_id)
	time_avg_recovery.insert(1, 'match_id',match_id)
	time_avg_recovery.rename(columns={'possession_team':'team'},inplace=True)
	insert_df_to_sql(time_avg_recovery,'time_off_the_ball')



def extract_locations_data(query,season_id):
	locations=pd.DataFrame(get_data(query),columns=['match_id','play_id','team_name','player_name','location_x','location_y'])
	locations.dropna(inplace=True)
	locations.rename(columns={'team_name':'team'},inplace=True)
	locations.insert(0, 'season_id',season_id)
	insert_df_to_sql(locations,'locations')



def extract_recoveries_players(query,season_id):
	player_recoveries=pd.DataFrame(get_data(query),columns=['match_id','player_name','recoveries'])
	player_recoveries.insert(0, 'season_id',season_id)
	insert_df_to_sql(player_recoveries,'recoveries')



def main():
	seasons=[1,2,4,21,22,23,24,25,26,27,39,40,41]
	#seasons=get_data(SEASON_SQL)
	for season_id in seasons:
		matches=get_data(GAMES_SQL%(season_id))
		for match in matches:
			extract_time_off_the_ball(PLAY_SQL%{'0':match['match_id']},season_id,match['match_id'])
			extract_locations_data(RECOVERY_LOCATIONS_SQL%(match['match_id']),season_id)
			extract_recoveries_players(RECOVERY_PLAYERS_SQL%(match['match_id']),season_id)
			
if __name__=='__main__':
	main()
