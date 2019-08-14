
# Elastify
## Requirements
### Setting up elasticsearch python client
If you can, install the python elasticsearch package globally:
sudo pip3 install elasticsearch

In case you have to work on a (linux) machine without python-elasticsearch
installed globally, you can execute bash setup.sh to create a virtual environment
and source it with source EVE/bin/activate.

### Setting up elasticsearch server
Download and unzip elasticsearch
**** Set the $ES_HEAP_SIZE environment variable ****
(otherwise, RAM will be limited to 1GB)
start bin/elasticsearch 

## Managing indices
The most basic operations for managing your indices are now available
via indices.py.
* List all indices : python3 indices.py list
* Create index/indices: python3 indices.py create indexname [<filename>]
* Delete index/indices: python3 indices.py delete indexname
You can also create/delete multiple indices at once by providing
a comma seperated list python3 indices.py -c tfidf,cfidf,ctfidf,hctfidf.
Consider the optional <filename> argument of create,
since it allows you to directly use the mapping specified in 'index_settings/hctfidf.yaml'.

## Elastifying
Index files Use elastify.py to index .txt or .json files, remember to specify
index and document type.
In case of json files, the field names of the json file are used,
in case of txt files, the whole content is inserted as a 'content' field
(you can change this with -f 'otherfieldname').
The syntax (assuming path to be the path to the directory of single files):
python3 elastify.py index doctype path
Example: python3 elastify.py tfidf economics path/to/data_dir
You can specify '-u' to perform partial document updates INSTEAD OF index operations
(i.e. does not insert new data, when ID is not found).


### Options
Read more about them in python3 elastify -h

## Get Help
If unsure about command line parameters python3 elastify.py -h
will help.  And feel free to have a look inside the code, it should be quite
readable. Or just ask me ;)


# Quick tutorial on generating L2R feature files 


## Preliminaries
- Make sure ElasticSearch is running and the indices (datasets) are up
- for TREC (4+5) and NTCIR 2 certain input files are needed: topics file, documents files, relevance scores file. The document file contains the document text + id and is needed for the features which are not calculated by the ES engine but rather "on-the-fly" (all necessary files I have precomputed and can be found under: TODO)
- Switch to: `$ cd your_git_folder/moving/Code/elastify`
- Make sure you have the latest git master updates (assuming you are on the master branch): `$ git pull origin`
- Activate the virtualenv for elastify by running the following command: `$ source EVE/bin/active`
- Install the changes locally: `$ python setup.py install`
- There is one script for the datasets without goldstandard (GS) and one for datasets providing goldstandard
    + trainify_nogs (no goldstandard, e.g. economics)
    + trainify_no (with goldstandard, e.g. trec or ntcir)

## Preprocessing input files
Before one can run the elastify scripts for a dataset with GS provided, we need to preprocess the input files. Here are the requirements for the formats (remove header columns beforehand!):

- goldstandard file:        `topicID \t docID \t score`
- topics (=queries) files:  `topicID \t topicText`
- document files:           `docID \t docText`

The python lib [pandas](http://pandas.pydata.org/pandas-docs/stable/#) can be a very handy tool for preprocessing data.

## Note on the parameters

### without GS
- queryfile:        query file with one query per line
- -i/--index:       the index
- -f/--field:       field of the index
- -d/--doctype:     doctype of the index
- -I/--gold_index:  the index used for the goldstandard retrieval
- -g/--gold-strategy:      the strategy for the goldstandard (default: fulltext)
- -s/--strategies:  the strategies to use
- -S/--size:        Number of documents per query for non-gold strategies
- -l/--lindex:      the index for the language models
- -F/--lfield:      the field for the language model index
- -B/--batches:     batches for the goldstandard
- -o/--outfile:     path to write the output
- -T/--type:        type of output (l2r, txt, etc)
- -w/--w2vmodel:    path to the word2vec model file
- -m/--mu:          mu parameter for mk language model score
- -x/--let_mu:      mu parameter for letor language model with dirichlet smoothing
- -y/--let_alpha:   alpha parameter for letor language model with jelinek-mercer smoothing
- -z/--let_delta:   delta parameter for letor language model with absolute discounting smoothing

### with GS
- queryfile:        query file with one query per line in format given above
- docsfile:         file containing all documents with corresponding id in format given above
- goldfile:         file containing all relevances scores in format given above
- -i/--index:       the index
- -f/--field:       field of the index
- -d/--doctype:     doctype of the index
- -s/--strategies:  the strategies to use
- -S/--size:        Number of documents per query for non-gold strategies
- -l/--lindex:      the index for the language models
- -F/--lfield:      the field for the language model index
- -B/--batches:     batches for the goldstandard
- -o/--outfile:     path to write the output
- -T/--type:        type of output (l2r, txt, etc)
- -w/--w2vmodel:    path to the word2vec model file
- -m/--mu:          mu parameter for mk language model score
- -x/--let_mu:      mu parameter for letor language model with dirichlet smoothing
- -y/--let_alpha:   alpha parameter for letor language model with jelinek-mercer smoothing
- -z/--let_delta:   delta parameter for letor language model with absolute discounting smoothing



## Example calls

### Example: NTCIR2 with titles
```shell
gstrainify /path/to/topics/topics.txt /path/to/titles/titles.txt
```
```shell
/path/to/relscores/rel_scores.txt -i ntcir_titles -f title -d publication 
```
```shell
-s mk sm letor ntcir_cfidf ntcir_hcfidf ntcir_bm25 -l ntcir_fulltext
```
```shell 
-F fulltext -m 10.0 -x 2000.0 -y 0.1 -z 0.7
```
```shell
-w /path/to/w2vmodel/GoogleNews-vectors-negative300.bin -T l2r 
```
```shell
-o /path/to/output/titles_features.txt
```

### Example: TREC 4+5 with fulltext
```shell
gstrainify /path/to/topics/topics.txt /path/to/fulltext/fulltext.txt
```
```shell 
/path/to/relscores/rel_scores.txt -i trec_fulltext -f fulltext -d
```
```shell
publication -s mk sm letor trec_cfidf_f trec_hcfidf_f trec_bm25_f -l trec_fulltext
```
```shell 
-F fulltext -m 10.0 -x 2000.0 -y 0.1 -z 0.7 -w
```
```shell 
/path/to/w2vmodel/GoogleNews-vectors-negative300.bin 
```
```shell
-T l2r -o /path/to/output/fulltext_features.txt
```

### Example:  ZBWEconomics with titles
```shell
nogstrainify -i economics -f title -d publication -g fulltext -I economics -s mk sm letor
```
```shell 
cfidf hcfidf bm25 -S 100 -l economics -m 10.0 -x 2000.0 -y 0.1 -z 0.7
```
```shell 
-w /path/to/w2vmodel/GoogleNews-vectors-negative300.bin 
```
```shell
-T l2r -o /path/to/output/feature_file.txt
```

## Learning to Rank
As soon as the feature files have been generated, one can start to run RankLib experiments. Here I provide sample calls ( [Link to RankLib How-To](https://sourceforge.net/p/lemur/wiki/RankLib%20How%20to%20use/) ):
```shell 
java -jar RankLib.jar -train path/to/feature/file.txt -ranker 1 
```
```shell
-metric2t MAP -tvs 0.75 -kcv 5
```
That will use RankNet (*ranker 1*), **m**ean **A**verage **P**recision as optimization and evaluation metric, a training-validation split of 0.75-0.25 (*tvs*) and 5-fold cross-validation (*kcv*)

If we want to use a subset of the features, one has to provide an additional feature file (one line = nr of one feature):
```shell
java -jar RankLib.jar -train path/to/feature/file.txt -ranker 1
```
```shell
-feature path/to/featuresubset/file.txt -metric2t MAP -tvs 0.75 -kcv 5
```

## Creating the Correlation-based feature subset scores
For finding a meaningful subset of the whole feature list, I wrote a script which evaluates all possible combinations of all sizes of a feature set and reports the best scoring feature subset for each size. The file can be found in the moving git under Code/elastify/feat_sel/cc.py .
The following parameters exist

- input: the input feature file in RankLib format
- -F/--features [optional]: file with feature names in right order line by line, if not provided the column nrs will be used to identify the features
- -f/--from_nr [optional]: size of subsets to start with, default = 1
- -t/--to_nr [optional]: size of subsets to end with (including), default = 0 (0 will be evaluated to length of feature list)
- -o/--output [optional]: output file path to write the results to, default=stdout
- -O/--type [optional], the format output type, default='csv', choices=['csv','tsv']

Example call:
```python
python cc.py path/to/feature/file.txt --features path/to/features.txt -f 28 -t 29
```


## To be continued
