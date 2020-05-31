create table laliga.competitions
(
competition_id int not null,
season_id int not null,
competition_name varchar(40),
season_name varchar(40),
PRIMARY KEY(competition_id,season_id)
);

create table laliga.games
(
match_id int not null,
match_date date,
kick_off varchar(40),
competition_id int,
season_id int,
home_team_name varchar(40),
away_team_name varchar(40),
home_score int,
away_score int,
PRIMARY KEY(match_id),
FOREIGN KEY(competition_id,season_id) REFERENCES laliga.competitions(competition_id,season_id)
);


create table laliga.plays
(
match_id int not null,
play_id int not null,
period int,
minute int,
second int,
play_type varchar(40),
play_pattern varchar(40),
possession_team varchar(40),
player_name varchar(100),
team_name varchar(100),
location_x double,
location_y double,
recovery_outcome varchar(10),
PRIMARY KEY(match_id,play_id),
FOREIGN KEY(match_id)references laliga.games(match_id)
);


create table laliga.time_off_the_ball
(
	season_id int,
    match_id int,
	team varchar(40),
	val double,
	PRIMARY KEY(season_id,match_id,team)
);


create table laliga.locations
(
  season_id int,
    match_id int,
    play_id int
  team varchar(40),
  player_name varchar(100),
  location_x double,
  location_y double,
  PRIMARY KEY(season_id,match_id,play_id)
);

create table laliga.recoveries
(
  season_id int,
    match_id int,
  player_name varchar(100),
  recoveries int,
  PRIMARY KEY(season_id,match_id,player_name)
);


