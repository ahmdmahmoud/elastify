import zlib
import os, sys
from subprocess import call, check_output
from html.parser import HTMLParser

prefix = '/data4/commondata/TREC/TREC_VOL5'
exclude = set(['dtds', 'aux'])
rename_fr94 = False

output = '/data4/commondata/TREC/TREC_4_5_converted/'
fail = output + 'fail.txt'

def get_doc_type(path):
	configs = {
		'cr': {
			'docid': 'docno',
			# the fields which need to be extracted
			# every field has a triple: name, tag in sgml, list tags to be excluded
			'fields': [('fulltext', 'text', ['ttl']) , ('title', 'ttl', [])]
		},

		'fr94': {
			'docid': 'docno',
			'fields': [('fulltext', 'text', ['doctitle']) , ('title', 'doctitle', [])]
		},

		'ft': {
			'docid': 'docno',
			'fields': [('fulltext', 'text', ['headline']) , ('title', 'headline', [])]
		},

		'fbis': {
			'docid': 'docno',
			'fields': [('fulltext', 'text', ['h3']) , ('title', 'h3', [])]
		},

		'latimes': {
			'docid': 'docno',
			'fields': [('fulltext', 'text', ['headline']) , ('title', 'headline', [])]
		}
	}

	path_elems = set(path.split(os.sep))
	tmp = set(configs.keys()).intersection(path_elems)
	if len(tmp) == 1:
		return configs[list(tmp)[0]]
	else:
		print('Unknown document type from TREC4-5')
		sys.exit(0)

def write_document(record, doc_type, destinations):
	docid = ''
	if 'docid' in record:
		docid = record['docid'].strip()
	else:
		print('no docid found!', record)
		return

	# we just need files who have all fields
	all_fields_present = True
	for field,_ in destinations.items():
		if field not in record:
			all_fields_present = False
			with open(fail, 'a') as fail_file:
				fail_file.write(record['docid'].strip() + '\n')
	if all_fields_present:
		for field, path in destinations.items():
			filepath = path + docid + '.txt' 
			if not os.path.isfile(filepath):
				with open(filepath, 'w') as doc_file:
					doc_file.write(record[field].strip())
			else:
				print('file already exists!', filepath)

class TRECDocumentParser(HTMLParser):

	def __init__(self, doc_type, field_destinations):
		self.num_records = 0
		self.tag_stack = []
		self.doc_type = doc_type
		self.field_destinations = field_destinations
		HTMLParser.__init__(self)

	def __is_active(self, tag):
		return tag in self.tag_stack

	def __are_active(self, taglist):
		for t in taglist:
			if self.__is_active(t):
				return True
		return False

	def handle_starttag(self, tag, attrs):
		self.tag_stack.append(tag)

		# clear record
		if tag == "doc":
			self.record = {}
			self.num_records += 1

	def handle_endtag(self, tag):
		self.tag_stack.pop()
		# write document into own file
		if tag == "doc":
			write_document(self.record, self.doc_type, self.field_destinations)


	def handle_data(self, data):
		if len(self.tag_stack) != 0:
			# handle the doc id
			id_tag = self.doc_type['docid']
			if self.tag_stack[-1] == id_tag:
				if 'docid' in self.record:
					self.record['docid'] = self.record['docid'] + data
				else:
					self.record['docid'] = data
			# handle the different fields
			for (name, tag, excludes) in  self.doc_type['fields']:
				if self.__is_active(tag) and not self.__are_active(excludes):
					if name in self.record:
						self.record[name] = self.record[name] + data
					else:
						self.record[name] = data

	def get_count(self):
		return self.num_records

def main():
	# check if fulltext and titles dirs exist and create
	fulltext_path = output + '/fulltext'
	titles_path = output + '/titles'
	if not os.path.exists(fulltext_path):
		os.makedirs(fulltext_path)
		print('creating dir')
	#else:
	#	sys.exit('Fulltext path already exists! Exiting ..')
	if not os.path.exists(titles_path):
		os.makedirs(titles_path)
		print('creating dir')
	#else:
	#	sys.exit('Titles path already exists! Exiting ..')

	# rename the misnamed files of fr94 (has to be done only once)
	if rename_fr94:
		for root, dirs, files in os.walk(prefix):
			path = root.split(os.sep)
			for file in files:
				# file should not be in excluded directories
				if len(exclude.intersection(set(path))) == 0:
					if 'fr94' in os.path.join(root,file):
						if len(file.split('.')[-1]) == 2 and file[-1] == 'z':
							new_name = os.path.join(root,file).replace('.', '').replace('z', '', -1)
							new_name = new_name + '.z'
							call(['mv', os.path.join(root,file), new_name])


	file_names = []
	# find all gzipped files
	for root, dirs, files in os.walk(prefix):
		path = root.split(os.sep)
		for file in files:
			# file should not be in excluded directories
			if len(exclude.intersection(set(path))) == 0:
				if file.split('.')[-1] == 'z':
					file_names.append(os.path.join(root,file))

	print('Found {} gzipped files'.format(len(file_names)))

	FIELD_DEST= {
		# for the title data
		'title': titles_path + '/',
		# for the fulltext data
		'fulltext': fulltext_path + '/'
	}

	docs = 0
	#file_names = [f for f in file_names if 'fr94' in f]
	for f in file_names:
		#print(f)
		result = check_output(['gzip', '-dck', f]).decode(encoding='utf-8', errors='ignore')
		parser = TRECDocumentParser(get_doc_type(f), FIELD_DEST)
		parser.feed(str(result))
		docs += parser.get_count()
	print('Converted {} unique documents'.format(docs))
	#fail_lines = check_output(['cat', f, '|' ,'wc', '-l'])
	#print('But {} failed'.format(int(fail_lines)/docs))



if __name__ == "__main__":
	main()
