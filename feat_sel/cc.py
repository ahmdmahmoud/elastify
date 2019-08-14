import sys, os
import scipy.stats as stats
import numpy as np
from io import StringIO
import pandas as pd
import itertools
import math
from operator import itemgetter
import argparse

def findsubsets(S,m):
    return set(itertools.combinations(S, m))

def main():
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument("input", help="The input feature file for the correlation-score calculation")
	parser.add_argument("-F,", "--features", help="The file which contains the features in the right order one per line", default=None)
	parser.add_argument("-f", "--from_nr", help="Size of subsets to start with", default=1, type=int)
	parser.add_argument("-t", "--to_nr", help="Size of subsets to end with", default=0, type=int)
	parser.add_argument("-o", "--output", help="output file to write the results", default=sys.stdout, type=argparse.FileType('w'))
	parser.add_argument("-O", "--type", default='csv', help="The format output type")
	args = parser.parse_args()

	# check if input is a file
	if not os.path.isfile(args.input):
		print('Please provide a valid input file in the RankLib format')
		sys.exit(2)

	columns = ['rel_score', 'qid']
	
	# we use the first line of the input file to determine number of cols
	with open(args.input) as inp:
		head = next(inp)
	col_nr = len(head.split(' '))

	# check if feature is a file
	feat_present = False
	
	if args.features and os.path.isfile(args.features):
		# read line by line the feature names 
		with open(args.features, 'r') as f:
			for line in f:
				columns.append(line.strip())
		if col_nr > len(columns):
			columns.extend(list(range(len(columns), col_nr)))
		feat_present = True 
	else:
		print('No valid feature file provided, continuing with column nrs')
		columns.extend(list(range(1, col_nr - 1)))

	
	df = pd.read_csv(args.input, delimiter=' ', names=columns)
	df.fillna(0, inplace=True) 
	
	if feat_present:
		iterator = columns[2:]
	else:
		iterator = list(range(1, len(columns[2:]) + 1))

	# clean up the data by removing comments etc
	to_drop = []
	for i in iterator:
		if not np.issubdtype(df[i], np.number):
			if df[i].str.startswith('#').all():
				df.drop(i, axis=1, inplace=True)
				to_drop.append(i)
	# drop the removed columns from iterator
	for i in to_drop:
		iterator.remove(i)

	# parsing the feature values
	for i in iterator:
		#check if feature is prepended with feature number and then remove and convert to float
		if not np.issubdtype(df[i], np.number):
			if df[i].str.contains(':').all():
				df[i] = df[i].str.split(pat=':', n=1).apply(lambda x: x[1]).astype(float)

	# cleaning qid
	if df['qid'].str.startswith('qid:').all():
		df['qid'] = df['qid'].str.split(pat=':', n=1).apply(lambda x: x[1])

	df.sort_values(by='qid', axis=0, inplace=True)

	# make sure the rel score is in the right format
	df['rel_score'] = df['rel_score'].astype(int)
	nr_features = len(df.columns) - 2

	# parsing 'to' and 'from' parameter
	from_nr = args.from_nr
	to_nr = args.to_nr
	if to_nr == 0:
		to_nr = nr_features

	if from_nr > nr_features or from_nr < 1:
		print('Either "from"-parameter greater than nr of features or smaller than 1')
		from_nr = 1
	if to_nr > nr_features or to_nr < 1 or to_nr < from_nr:
		print('Either "to"-parameter greater than nr of features, smaller than 1 or smaller than "from"-parameter ',nr_features)
		to_nr = nr_features

	#print('nr of features:', nr_features)
	#print('from_nr:', from_nr)
	#print('to_nr:', to_nr)
	feature_list = list(df.columns[2:])

	ff_corrs = df[feature_list].corr(method='pearson')
	fc_corrs = pd.DataFrame([0.0]*len(feature_list), columns=['rel_score'], index=feature_list)
	fc_corrs.fillna(0, inplace=True) 
	for f in feature_list:
		fc_val = df[['rel_score', f]].corr(method='pearson')[f][0]
		if np.isnan(fc_val): fc_val=0 #overwrite the nan elements 
		#print ("fc_val: ", fc_val )
		fc_corrs.loc[f,'rel_score'] = fc_val

	best_subsets = []
	for k in range(from_nr, to_nr + 1):
		print('starting subset size:', k)
		subsets = list(findsubsets(feature_list, k))
		merits = []
		if k == 1:
			for subset in subsets:
				for fi in subset:
					merits.append((fc_corrs.at[fi, 'rel_score'], subset))
		else:
			for subset in subsets:
				checked = []
				ff_corrs_avg = 0.0
				fc_corrs_avg = 0.0
				ff_i = 0
				for fi in subset:
					fc_corrs_avg += fc_corrs.at[fi,'rel_score']
					for fj in subset:
						if fi is not fj and fj not in checked:
							#print('checking:', fi, 'and', fj, 'with', ff_corrs.at[fi,fj])
							ff_i += 1
							ff_corrs_avg += ff_corrs.at[fi,fj]
					checked.append(fi)
				ff_corrs_avg = ff_corrs_avg/(ff_i) 
				if np.isnan(ff_corrs_avg): ff_corrs_avg = 0 #if it's nan, convert to zero 
				fc_corrs_avg = fc_corrs_avg/len(subset)
				merit = (k * fc_corrs_avg) / (math.sqrt(k + k * (k - 1) * ff_corrs_avg)+0.00001) #prevent deviding by zero
				if np.isnan(merit): print ("ff_corrs_avg", ff_corrs_avg, "  and k: ", k, "ff_i", ff_i) 
				merits.append((merit, subset))
		best_subsets.append(sorted(merits, key=itemgetter(0), reverse=True)[0])
	
	# TODO doesnt work with no features file and features parameter something wrong

	if args.type == 'tsv':
		print('size\tscore\tsubset', file=args.output)
		for merit, ss in best_subsets:
			tmp = str(len(ss)) + '\t'+ str(merit) + '\t'+ str(ss)
			print(tmp, file=args.output)
	else:
		print('size;score;subset', file=args.output)
		for merit, ss in best_subsets:
			tmp = str(len(ss)) + ';'+ str(merit) + ';'+ str(ss)
			print(tmp, file=args.output)

if __name__ == "__main__":
    main()
