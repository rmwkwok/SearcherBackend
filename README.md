# Backend

### Introduction
This implements a searching mechanism for a search engine backed by two inverted indexes created with Lucene and Hadoop.

### System schematics
![alt text](https://github.com/rmwkwok/searcherBackend/blob/015605f6b6a2406d5af28c962f8991a1df9283e9/architecture.png)

### Search machanism
It first expanded query words to include similar words pre-determined by cosine similarity of word embeddings (of size 300) trained with a corpus of English Wikipedia Dump of February 2017 (Ref 1) that contained 273992 words. 

The expanded words were tokenized in the same way as indexing (e.g. EnglishAnalyzer for Lucene). Then, for Lucene, the tokens went to a MultiFieldQueryParser which built a Boolean Query Parser that used the fields of “content”, “title”, and “anchor text” in an OR manner, whereas for Hadoop, the tokens were used to match the inverted index of content words. Both Lucene and Hadoop approaches used BM25 for evaluating and sorting the documents.

A set of documents of required numbers (N=10) plus extra (30) were returned for scoring (of 4 dimensions), while only the top-N scored (in combined) would be chosen and returned with snippets to the frontend. Users who found the snippet of a document interesting might bias the next search result to the document by a new query which contained both query words and the document’s ID. A set of document's embedding centroids were pre-computed as the weighted-averaged of embeddings of words having more than 4 characters. 

### Snippet generation
A snippet was for previewing a relevant part of a page, and was a continuous trunk of between 60 and 90 words, containing the most number of expanded query words at the least trunk size.
It ran a brute force search of such a trunk at complexity of O((M-1)(M-2)) where M is the number of matched terms (instead of total size) of the document and was usually too small to impact the speed. Positions of the matched terms could be easily obtained from the inverted index with position.
The search was conducted breath-wise so that for each level of n, where n is the number of matched terms in a candidate trunk, the best trunk was selected to compare to the next level, until a level which produced no equal or better trunk, then the last best trunk became the snippet. The advantage of a breath-wise search was that the number of matched terms at each level was always the same, so comparison of term density of trunks among the same level could be reduced to comparison of trunk length so that doing division to get density was avoided.

![alt text](https://github.com/rmwkwok/searcherBackend/blob/928bbd345a57de402ebb66715175f68157d92d04/snippetExample.png)

### Deployment
A jar file is available in the ./target folder, but as indicated in the init.sh script, the deployment requires the locations for the inverted indexes, and word2vec files which are not available in this repo.

### Reference
1. Word embedding source. Retrieved from http://vectors.nlpl.eu/repository/, ID = 5.
