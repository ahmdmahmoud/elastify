import pandas as pd
import numpy as np
import os,sys
import pickle

# choose these values so they are conform with the specifications in the table in section 5.2 of the NTCIR2 manual
#DOCUMENTS_DIRECTORY = "/data/asaleh/NTCIR2/documents_files_ntc1-ntc2-all"
DOCUMENTS_DIRECTORY = "/home/devnull/Development/python/masterproject/data/NTCIR_all/fulltext"
#TOPICS_DIRECTORY = "/data/asaleh/NTCIR2/topics_long"
TOPICS_DIRECTORY = "/home/devnull/Development/python/masterproject/data/NTCIR2/topics/topics_short"
#RELEVANCE_FILE = "/data/asaleh/NTCIR2/rels/rel2_ntc2-e2_0101-0149"
RELEVANCE_FILE = "/home/devnull/Development/python/masterproject/data/NTCIR2/rels/rel2_ntc2-e2_0101-0149"

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

    # this is what the relevance judgment file looks like according to the NTCIR2 manual (Section 5.3)
    relevance = pd.read_csv(relevance_file_name, sep = '\t', header = None, names = ["topic-ID", "dummy", "document-ID", "relevance-judgement", "comments"], dtype = str)
    total = 0
    success = 0
#        relevance_matrix[map(index_of_pub, relevant_publications["publication_id"].values), i] = 1
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
    np.save("./NTCIR2_relevance_matrix.npy", relevance_matrix, allow_pickle=False)
    #with open(, 'wb') as outfile:
    #    pickle.dump(relevance_matrix, outfile, pickle.HIGHEST_PROTOCOL)
    
