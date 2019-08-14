from os import listdir
from os.path import isfile, join

#doc_folder = '/home/devnull/Development/python/masterproject/data/NTCIR_all/titles'
#output = '/home/devnull/Development/python/masterproject/data/NTCIR_all/titles_all.txt'
doc_folder = '/data4/commondata/TREC/TREC_4_5_converted/titles'
output = '/data4/commondata/TREC/TREC_4_5_converted/titles_all.txt'
# doc paths
with open(output, 'w') as f:
	for d in listdir(doc_folder):
		doc_path = join(doc_folder, d)
		doc_id = d.split('.')[0]
		tmp = open(doc_path, 'r')
		text = tmp.read()
		try:
			doc_text =  text.replace('\n', ' ')
			f.write(doc_id + '\t' + doc_text + '\n')
		except:
			print('Couldnt strip id:', doc_id)
