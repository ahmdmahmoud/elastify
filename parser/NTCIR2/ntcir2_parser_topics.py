from HTMLParser import HTMLParser
import sys, os

## can currently only fields with pure text but not multiple elements inside field (like for field: concept)

# create a subclass and override the handler methods
class NTCIRTopicsParser(HTMLParser):

    def __init__(self, field, outpath):
        self.field = field
        if not outpath.endswith('/'):
            self.outpath = outpath + '/'
        else:
            self.outpath = outpath
        self.num_topics = 0
        HTMLParser.__init__(self)

    def handle_starttag(self, tag, attrs):
        self.current_tag = tag
        self.tag_ended = False

        # clear record
        if tag == "topic":
            self.record = {}
            if not len(attrs) == 1:
                print("False assumption: topic has no id")
                sys.exit(0)
            attribute, value = attrs[0]
            if not attribute == "q":
                print("False assumption: wrong attribute")
                sys.exit(0)
            self.record['qid'] = value
        
        # count number of paragraphs
        if tag == "topic":
            self.num_topics += 1

    def handle_endtag(self, tag):
        self.tag_ended = True

        # write document into own file
        if tag == "topic":
            self.write_document(self.record)


    def handle_data(self, data):
        data = data.strip()
        if not self.tag_ended:
            if self.current_tag == self.field:
                    if self.field in self.record:
                        self.record[self.field] = self.record[self.field] + data
                    else:
                        self.record[self.field] = data

    def write_document(self, record):
        f = open(self.outpath + record['qid'] + '.txt', 'w')
        f.write(record[self.field])

    def get_count(self):
        return self.num_topics


def main():
    """Script to generate training data for learning 2 rank
    :returns: TODO

    """
    fields = ['title', 'description', 'narrative', 'concept', 'field']
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--path', type=argparse.FileType('r'),
                        help="The path to the topics file", required=True)
    parser.add_argument('-f', '--field', default='description',
                        help="The field to extract from the topics file", choices=fields)
    parser.add_argument('-o', '--outpath', default='./topics', help="Path for output [./topics]")
    args = parser.parse_args()
    #print(args)
    document = args.path.read()

    parser = NTCIRTopicsParser(args.field, args.outpath)
    parser.feed(document)
    print('Topics found: ', parser.get_count())

if __name__ == "__main__":
    main()