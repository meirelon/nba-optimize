import argparse
from datetime import datetime
from pulp import *
import numpy as np
import pandas as pd
import re
import csv
import random
from player import *

from prod_utils import load_pipeline
import FeatureBuilding

class DraftKingsNBAOptimizeLineups:
	def __init__(self, project, dataset, dk_link, total_lineups, season, partition_date):
		self.project = project
		self.dataset = dataset
		self.dk_link = dk_link
		self.total_lineups = total_lineups
		self.season = season
		self.partition_date = partition_date
		self._projection_df = None

	@property
	def get_projections(self):
		if not self._projection_df:
			build_features = FeatureBuilding.BuildFeatureSet(project=self.project,
											bucket='draftkings',
											destination_path='sql_queires/training',
											filename='get_player_data',
											season=self.season,
											partition_date=self.partition_date,
											is_today=True)
			df = build_features.get_feature_df()
			# query = pd.read_pickle("../query.pkl")
			# model = pd.read_pickle("model.pkl")


			# df = pd.read_gbq(prepared_query, project_id=self.project, dialect="standard", verbose=False).fillna(value=0)
			df = df.set_index("player")
			prediction_input = df.select_dtypes([np.number]).drop(['dk', 'secs_played'], axis=1).dropna()

			model = load_pipeline(project_id=self.project,
									bucket='draftkings',
									destination_path='nba_models/{partition_date}'.format(partition_date=self.partition_date),
									filename='model')
			df["Projected"] = model.predict(prediction_input)
			self._projection_df = df
			return self._projection_df

	def optimize(self):
		projection_df = self.get_projections
		iterations=50
		data_col_names = ["Id", "Name", "Position", "Team", "Salary", "AvgPointsPerGame"]
		dk_data = pd.read_csv(self.dk_link)[["ID", "Name", "Position", "TeamAbbrev", "Salary", "AvgPointsPerGame"]]
		dk_data.columns = data_col_names
		dk_data = dk_data.set_index("Name")
		data = dk_data.join(projection_df["Projected"], how="left").reset_index().drop(["AvgPointsPerGame"], axis=1).fillna(value=0)
		prob = pulp.LpProblem('NBA', pulp.LpMaximize)
		players={}
		total_budget=50000
		pgs=sgs=sfs=pfs=cs=''
		objective_function=''
		total_cost=''
		decision_variables=[]
		num_players=''
		for rownum, row in data.iterrows():
			variable = str('x' + str(rownum))
			variable = pulp.LpVariable(str(variable), lowBound = 0, upBound = 1, cat= 'Integer')
			player=Player(row, str(variable))
			players[str(variable)]=player
			decision_variables.append(variable)
			num_players += variable

			player_points = row["Projected"]*variable
			objective_function += player_points

			player_cost = row['Salary']*variable
			total_cost+= player_cost

			#Categorize players by position groups
			pgs += player.position['PG']*variable
			sgs += player.position['SG']*variable
			sfs += player.position['SF']*variable
			pfs += player.position['PF']*variable
			cs += player.position['C']*variable
		#Set  the objective function
		prob +=  lpSum(objective_function)


		#Mininum constraints for an eligible lineup
		prob += (total_cost <= total_budget)
		prob += (num_players ==8)

		prob += (pgs <=3)
		prob += (pgs >=1)

		prob += (sgs <=3)
		prob += (sgs >=1)

		prob += (sfs <=3)
		prob += (sfs >=1)

		prob += (pfs <=3)
		prob += (pfs >=1)

		prob += (cs <=2)
		prob += (cs >=1)

		#additional Constraint
		diversity_constraint=''
		lineups=[]
		# iterations=self.total_lineups
		for i in range(1,self.total_lineups+1):
			optimization_result = prob.solve()
			if optimization_result != pulp.LpStatusOptimal:
				print("finished abrupty")
				break
			lineup=[]
			selected_vars=[]
			diversity_constraint=''
			freq_limit=7
			div_limit=3
			lineup_values=[]
			for var in prob.variables():
				if 'x' not in str(var):
					continue
				if var.varValue:

					selected_vars.append(var)
					player=players[str(var)]
					lineup.append(player)
					#print player.name, player.scored, player.projected
					player.count+=1
					frequency_constraint=''
					frequency_constraint+=player.count*var+var
					prob+=(frequency_constraint<=freq_limit)
					#Resets the value to be 'fresh' for next optimization
					var.varValue=0
				#Force diversity s.t no than two lineups can share more than 3 players
			diversity_constraint=sum([var for var in selected_vars])
			prob+=(diversity_constraint<=div_limit)

			lineups.append(lineup)
		filename="Prediction.csv"
		self.write_output(filename,lineups,prob)

	def write_output(self, filename, lineups, prob):
		#Writes lineups to csv
		player_list=[]
		team_list=[]
		salary_list=[]
		for i in range(8):
			player_list.append('Player%s' %str(i+1))
			team_list.append('Team%s' %str(i+1))
			salary_list.append('Salary%s' %(str(i+1)))
		target=open(filename, 'w')
		headers=player_list+team_list+salary_list+['Projected_Value', 'Iteration']
		target=open(filename, 'w')
		csvwriter=csv.writer(target)
		csvwriter.writerow(headers)
		for iteration, lineup in enumerate(lineups):
			names=[]
			teams=[]
			salaries=[]
			projected=[]
			for player in lineup:
				names.append(player.name)
				teams.append(player.team)
				projected.append(player.projected)
				salaries.append(player.salary)
			counter=collections.Counter(teams)

			final_output=names+teams+salaries+[round(sum(projected),2), iteration+1]
			csvwriter.writerow(final_output)
		target.close()

		df=pd.read_csv(filename)
		df.to_gbq(project_id=self.project,
		destination_table="{dataset}.projections_{dt}".format(dataset=self.dataset, dt=datetime.today().strftime("%Y%m%d")),
		if_exists="replace")

def main(argv=None):
	parser = argparse.ArgumentParser()

	parser.add_argument('--project',
	                    dest='project',
	                    default = 'scarlet-labs',
	                    help='This is the GCP project you wish to send the data')
	parser.add_argument('--dataset_id',
	                    dest='dataset_id',
	                    default = 'draftkings',
	                    help='This is the sport type (for now)')
	parser.add_argument('--total_lineups',
	                    dest='total_lineups',
	                    default = 50,
	                    help='How many lineups should be generated?')
	parser.add_argument('--season',
	                    dest='season',
	                    default = 2019,
	                    help='which season is this for?')
	parser.add_argument('--partition_date',
	                    dest='partition_date',
	                    default = None,
	                    help='latest partition date')
	parser.add_argument('--dk_link',
	                    dest='dk_link',
	                    default = None,
	                    help='Link to Draft Kings Players')

	args, _ = parser.parse_known_args(argv)

	optimize_pipeline = DraftKingsNBAOptimizeLineups(project=args.project,
													dataset=args.dataset_id,
													dk_link=args.dk_link,
													total_lineups=args.total_lineups)
	optimize_pipeline.optimize()


if __name__ == '__main__':
	main()
