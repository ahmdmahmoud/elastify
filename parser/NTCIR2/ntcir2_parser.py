from HTMLParser import HTMLParser
import sys, os

DESTINATION = "/data/asaleh/NTCIR2/doc_files/"

# use one of the following paths for documents
#DOCUMENTS_FILE = "/data/asaleh/NTCIR2/e-docs/ntc2-e1k"
#DOCUMENTS_FILE = "/data/asaleh/NTCIR2/e-docs/ntc2-e1g"
DOCUMENTS_FILE = "/data/asaleh/NTCIR2/e-docs/ntc1-e1.mod"

# use this path for topics
#DOCUMENTS_FILE = "/data/asaleh/NTCIR2/topics/topic-e0101-0149"

# use this opening tag for documents
RECORD_OPENING_TAG = "rec"

# use this opening tag for topics
#RECORD_OPENING_TAG = "topic"

# use a subset of these for ntc2-e1g or ntc1-e-mod
FIELDS = ["tite", "abse.p"]

# use a subset of these for ntc2-e1k
#FIELDS = ["pjne", "abse.p"]

# use a subset of these for topics (english)
#FIELDS = ["title"]

TYPE = "DOCUMENTS"
#TYPE = "TOPICS"

num_end_tags = 0
record_active = False

def write_document(record, identifier):
    file_path = DESTINATION + identifier + ".txt"
    if os.path.isfile(file_path):
        print("Overwriting file")
        sys.exit(0)
        
    document_file = open(file_path, "w")
    for field in FIELDS:
        document_file.write(record[field])
    document_file.close()
        

# create a subclass and override the handler methods
class NTCIRDocumentParser(HTMLParser):

    def handle_starttag(self, tag, attrs):
        self.current_tag = tag
        self.tag_ended = False
        
        # clear record
        if tag == RECORD_OPENING_TAG:

            global record_active
            if record_active == True:
                print("False assumption: Record starts before another one has ended.")
                sys.exit(0)

            self.record = {}
            self.num_paragraphs = 0

            # read identifier of topic
            if TYPE == "TOPICS":
                _, identifier = attrs[0]
                self.identifier = identifier

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
        if tag == RECORD_OPENING_TAG:
            global record_active
            record_active = False

            global num_end_tags
            num_end_tags += 1
            if TYPE == "DOCUMENTS":
                self.identifier = self.record["accn"]
            write_document(self.record, self.identifier)


    def handle_data(self, data):
        if not self.tag_ended:
            if self.current_tag == "abse.p":
                    if "abse.p" in self.record:
                        self.record["abse.p"] = self.record["abse.p"] + data
                    else:
                        self.record["abse.p"] = data
            else:
                self.record[self.current_tag] = data

# switch between these two for the two different datasets provided with NTCIR2
documents = open(DOCUMENTS_FILE, 'r').read()

parser = NTCIRDocumentParser()
parser.feed(documents)
parser.close()

print("Processed " + str(num_end_tags) + " files.")
