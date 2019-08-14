import pandas as pd
import numpy as np
import os,sys
import pickle

DOCUMENTS_DIRECTORY = "/data4/commondata/TREC/TREC_4_5_converted/fulltext"

TOPICS_DIRECTORY = "/data4/commondata/TREC/TREC_4_5_converted/topics/topics_title"

RELEVANCE_FILE = "/data4/commondata/TREC/TREC_4_5_converted/rel_scores/qrels_trec6_adhoc_all.txt"

FAIL = "/data4/commondata/TREC/TREC_4_5_converted/fail.txt"

OUTPUT = "/data4/commondata/TREC/TREC_4_5_converted/TREC_relevance_matrix.npy"

def load_documents(docs_path):
	documents = dict()
	docs_list = os.listdir(docs_path)
	for doc in docs_list:
		doc_id = doc.split('.')[0]
		documents[doc_id] = docs_path + '/' + doc

	return documents

def reduce_dicts(titles):
	""" reduce dictionary to list,
	providing 'same index' iff 'same key in the dictionary'
	"""
	titles = dict(titles)
	titles_list = []
	for key in titles:
		titles_list.append(key)

	titles_list.sort()
	return titles_list

def create_relevance_matrix(docs_list, topic_list, relevance_file_name):
	relevance_matrix = np.zeros((len(docs_list), len(topic_list)))

	failed = ''
	with open(FAIL, 'r') as f:
		failed = f.read()
	failed = failed.split('\n')
	# this is what the relevance judgment file looks like
	relevance = pd.read_csv(relevance_file_name, sep = ' ', header = None, names = ["topic-ID", "dummy", "document-ID", "relevance-judgement"], dtype = str)
	total = 0
	success = 0

	for _, row in relevance.iterrows():
		relevance_of_document_for_topic = int(row["relevance-judgement"])
		if relevance_of_document_for_topic == 1:

			topic_id = str(row["topic-ID"])
			topic_index = topic_list.index(topic_id)

			total += 1
			document_id = str(row["document-ID"])
			if document_id in docs_list:
				document_index = docs_list.index(document_id)
				success += 1
			if document_id in failed:
				success += 1
			else:
				document_index = -1

			#print("Document " + document_id + " is relevant for topic " + topic_id)
			#print("Topic Index " + str(topic_index) + " : " + str(document_index) + " Document Index")

			if document_index >= 0:
				relevance_matrix[document_index, topic_index] = 1

	print("Success rate: ", float(success) / float(total))
	return relevance_matrix

if __name__ == "__main__":
	# load documents and topics into two list that are lexicographically sorted by the ids
	# create a relevance matrix where the rows are documents and the columns are topics all sorted lexicographically

	print("Loading neccessary files.")
	docs_list = reduce_dicts(load_documents(DOCUMENTS_DIRECTORY))
	topic_list = reduce_dicts(load_documents(TOPICS_DIRECTORY))
	print('Found ', len(topic_list), ' topics')
	print('Found ', len(docs_list), ' documents')

	relevance_matrix = create_relevance_matrix(docs_list, topic_list, RELEVANCE_FILE)
	np.save(OUTPUT, relevance_matrix, allow_pickle=False)
	#with open(, 'wb') as outfile:
	#    pickle.dump(relevance_matrix, outfile, pickle.HIGHEST_PROTOCOL)
	
