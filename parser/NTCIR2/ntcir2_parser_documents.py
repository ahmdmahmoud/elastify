from HTMLParser import HTMLParser
import sys, os

DESTINATION = "/data/asaleh/NTCIR2/doc_files/"
#DOCUMENTS_FILE = "/data/asaleh/NTCIR2/e-docs/ntc2-e1k"
#DOCUMENTS_FILE = "/data/asaleh/NTCIR2/e-docs/ntc2-e1g"
#DOCUMENTS_FILE = "/home/devnull/Development/python/masterproject/data/NTCIR2/e-docs/ntc2-e1g"
DOCUMENTS_FILE = "/home/devnull/Development/python/masterproject/data/NTCIR1/clir/ntc1-e1"

# use a subset of these for ntc2-e1g
FIELDS = ["tite", "abse.p"]

# use a subset of these for ntc2-e1k
#FIELDS = ["pjne", "abse.p"]

FIELD_DEST_E1G = {
    # for the title data
    'title' : ('tite','/home/devnull/Development/python/masterproject/data/NTCIR2/e1g/titles/','gakkai-e-'),
    # for the fulltext data
    'fulltext': ('abse.p', '/home/devnull/Development/python/masterproject/data/NTCIR2/e1g/fulltext/','gakkai-e-')
}

FIELD_DEST_E1K = {
    # for the title data
    'title': ('pjne', '/home/devnull/Development/python/masterproject/data/NTCIR2/e1k/titles/', 'kaken-e-'),
    # for the fulltext data
    'fulltext': ('abse.p', '/home/devnull/Development/python/masterproject/data/NTCIR2/e1k/fulltext/', 'kaken-e-')
}

FIELD_DEST_E1 = {
     # for the title data
    'title' : ('tite','/home/devnull/Development/python/masterproject/data/NTCIR1/titles/','gakkai-'),
    # for the fulltext data
    'fulltext': ('abse.p', '/home/devnull/Development/python/masterproject/data/NTCIR1/fulltext/','gakkai-')
}

FIELD_DEST = FIELD_DEST_E1

## set this to False if you want to have separate
SEPARATE = True

def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text

def write_document(record, rm_prefix=False):
    if not SEPARATE:
        file_path = ''
        if rm_prefix:
            file_path = DESTINATION + remove_prefix(record['accn'], prefix) + ".txt"
        else:
            file_path = DESTINATION + record["accn"] + ".txt"
        if os.path.isfile(file_path):
            print("Overwriting file, exiting ..")
            sys.exit(0)
            
        document_file = open(file_path, "w")
        for field in FIELDS:
            document_file.write(record[field])
        document_file.close()
    else:
        for key, (field,path, prefix) in FIELD_DEST.items():
            if rm_prefix:
                path = path + remove_prefix(record['accn'], prefix)  + '.txt'
            else:
                path = path + record['accn']  + '.txt'
            if not os.path.isfile(path):
                doc_file = open(path, 'w')
                doc_file.write(record[field])
                doc_file.close()
                #sys.exit(0)


# create a subclass and override the handler methods
class NTCIRDocumentParser(HTMLParser):

    def __init__(self):
        self.num_records = 0
        HTMLParser.__init__(self)

    def handle_starttag(self, tag, attrs):
        self.current_tag = tag
        self.tag_ended = False

        # clear record
        if tag == "rec":
            self.record = {}
            self.num_records += 1
            self.num_paragraphs = 0

        # check assumptions about abse
        if tag == "abse":
            if not len(attrs) <= 1:
                print("False assumption: ABSE can have more than one attribute")
                sys.exit(0)
            attribute, value = attrs[0]
            if not attribute == "type":
                print("False assumption: type not the only attribute of abse but also: " + attribute)                
                sys.exit(0)
            if not value == "alpha":
                print("False assumption: alpha not the only abse type")
                sys.exit(0)
        
        # count number of paragraphs
        if tag == "abse.p":
            self.num_paragraphs += 1

    def handle_endtag(self, tag):
        self.tag_ended = True

        # write document into own file
        if tag == "rec":
            write_document(self.record)


    def handle_data(self, data):
        if not self.tag_ended:
            if self.current_tag == "abse.p":
                if "abse.p" in self.record:
                    self.record["abse.p"] = self.record["abse.p"] + data
                else:
                    self.record["abse.p"] = data
            elif self.current_tag == "tite":
                if "tite" in self.record:
                    self.record["tite"] = self.record["tite"] + data
                else:
                    self.record["tite"] = data
            elif self.current_tag == "pjne":
                if "pjne" in self.record:
                    self.record["pjne"] = self.record["pjne"] + data
                else:
                    self.record["pjne"] = data
            else:
                self.record[self.current_tag] = data

    def get_count(self):
        return self.num_records

# switch between these two for the two different datasets provided with NTCIR2
#with open(DOCUMENTS_FILE) as f:
#    print(sum(1 for _ in f))
f = open(DOCUMENTS_FILE, 'r')
documents = f.read()

parser = NTCIRDocumentParser()
parser.feed(documents)
f.close()
print(parser.get_count())
