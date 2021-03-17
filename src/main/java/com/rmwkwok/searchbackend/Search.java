package com.rmwkwok.searchbackend;

import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.json.simple.JSONArray;
import org.springframework.web.bind.annotation.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/search")
@CrossOrigin("*")
public class Search {

    final boolean doQueryExpansion = SearchbackendApplication.doQueryExpansion;
    final boolean doQueryModification = SearchbackendApplication.doQueryModification;
    final private int minSnippetWords = SearchbackendApplication.minSnippetWords;
    final private int maxSnippetWords = SearchbackendApplication.maxSnippetWords;
    final private int numSearchResult = SearchbackendApplication.numSearchResult;
    final private int queryExpandLimit = SearchbackendApplication.queryExpandLimit;
    final private int numExtraSearchResult = SearchbackendApplication.numExtraSearchResult;
    final private int closeCoOccurrenceCondition = SearchbackendApplication.closeCoOccurrenceCondition;

    final private Word2VecObj w2vObj;
    final private QueryParser queryParser;
    final private IndexReader indexReader;
    final private IndexSearcher indexSearcher;

    final private double bm25k1 = 1.2;
    final private double bm25k2 = 100.0;
    final private double bm25b = 0.75;

    final private Map<String, String> fileNameToDocID;
    final private Map<String, Map<String, Double>> hadoopIndex;

    Search() throws IOException {
        long startTime = System.nanoTime();

        // Lucene
        queryParser = new MultiFieldQueryParser(new String[] {"content", "title", "anchor_text"}, new EnglishAnalyzer());
        indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(SearchbackendApplication.indexFolder)));
        indexSearcher = new IndexSearcher(indexReader);
        indexSearcher.setSimilarity(new BM25Similarity());
        fileNameToDocID = new HashMap<>();

        for (int i = 0; i < indexReader.maxDoc(); i++)
            fileNameToDocID.put(indexReader.document(i).get("filename"), String.valueOf(i));

        // Hadoop
        hadoopIndex = initHadoopIndex();

        // Common
        w2vObj = new Word2VecObj(fileNameToDocID);

        long endTime = System.nanoTime();
        System.out.println("Search initialization took " + (endTime - startTime)/1e6 + "ms");
    }

    /**
     * initialize Hadoop index
     * @return Map<String, Map<String, Double>> hadoop index
     */
    private Map<String, Map<String, Double>> initHadoopIndex() throws IOException {
        Map<String, Map<String, Integer>> rawData = new HashMap<>();
        String _hadoopIndexPath = Paths.get("/home/raymondkwok/git/InformationRetrieval/hadoopIndex").toString();

        BufferedReader reader = new BufferedReader(new FileReader(_hadoopIndexPath));

        String line = reader.readLine();
        while (line != null) {
            String[] _line = line.split("\t");
            try {
                Map<String, Integer> map = new HashMap<>();
                Arrays.stream(_line[1].split(",")).forEach(s -> {
                    String[] _s = s.split(":");
                    map.put(_s[0], Integer.valueOf(_s[1]));
                });
                rawData.put(_line[0], map);
            } catch (Exception e) {
                System.out.println("Parse HadoopIndex error");
                System.out.println(line);
                e.printStackTrace();
                break;
            }
            line = reader.readLine();
        }
        reader.close();

        Map<String, Map<String, Double>> _hadoopIndex = new HashMap<>();

        // dl[doc] = length of document
        // N = |dl| = number of document
        Map<String, Integer> dl = new HashMap<>();
        rawData.forEach((w, v) -> v.forEach((d, f) -> dl.put(d,  f + dl.getOrDefault(d, 0))));

        double N = dl.size();
        double avdl = dl.values().stream().reduce(0, Integer::sum).doubleValue()/N;

        for (Map.Entry<String, Map<String, Integer>> e1 : rawData.entrySet()) {
            double n = e1.getValue().size();
            Map<String, Double> temp = new HashMap<>();
            e1.getValue().forEach((d, f) -> {
                if (fileNameToDocID.containsKey(d)) {
                    double K = bm25k1 * ((1 - bm25b) + bm25b * dl.get(d) / avdl);
                    temp.put(fileNameToDocID.get(d), Math.log((N - n + 0.5) / (n + 0.5)) * (bm25k1 + 1) * f / (K + f));
                }
            });
            _hadoopIndex.put(e1.getKey(), temp);
        }

        return _hadoopIndex;
    }

    /**
     * Test whether the term can be parsed. Added this because not all term can pass to Lucene.
     * @param queryTerm String
     * @return boolean
     */
    private boolean isParsable(String queryTerm) {
        try {
            queryParser.parse(queryTerm);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    /**
     * Get term positions in specific docID, using record of Lucene index.
     * For this method to work, Lucene must have enabled remembering term position at indexing stage
     * @param docID int
     * @param term String
     * @param filterCloseCoOccurrence boolean Whether or not to remove position which is too close,
     *                                for the use of determining CloseCoOccurrence score
     * @return List<Integer>
     */
    private List<TermPosition> getSingleTermPositionsSorted(int docID, String term, boolean filterCloseCoOccurrence) {
        List<TermPosition> termPositions = new ArrayList<>();
        try {
            PostingsEnum p = null;
            TermsEnum termsEnum = indexReader.getTermVector(docID, "content").iterator();
            while( termsEnum.next() != null ) {
                if (termsEnum.term().utf8ToString().equals(term)) {
                    p = termsEnum.postings(p, PostingsEnum.ALL);
                    while (p.nextDoc() != PostingsEnum.NO_MORE_DOCS)
                        for (int i = 0; i < p.freq(); i++)
                            termPositions.add(new TermPosition(term, p.nextPosition(), p.startOffset(), p.endOffset()));
                }
            }

            termPositions.sort(TermPosition::compareTo);

            if (filterCloseCoOccurrence) {
                getCloseCoOccurrenceIndex(termPositions).stream()
                        .sorted(Comparator.reverseOrder())
                        .forEach(i -> termPositions.remove((int) i)); // (int) remove ambiguity of autoboxing between interpreting i as an index or an Object to be removed
            }

            System.out.println("getTermPositionsSorted:: " + termPositions.size() + " times for " + term + " in docID " + docID);
        } catch (IOException e) {
            System.out.println("getTermPositionsSorted:: getTermVector:: IOException:: " + e);
        }

        return termPositions;
    }

    /**
     * Parsed term is useful when working words with Lucene indexing, since we need consistent transformation
     * of word with the index.
     * @param parser QueryParser Use same Analyzer as indexing step
     * @param queryString String
     * @return String[] a list of parsed term
     */
    private String[] getParsedTerms(QueryParser parser, String queryString) {
        // use the snippetParser.parse to do the parsing
        // however, I do not know how to get parsed terms properly
        // so use toString() and expect a list of field:term delimited by space
        try {
            return parser.parse(queryString).toString().split(" ");
        }
        catch (ParseException e) {
            return new String[0];
        }
    }

    /**
     * Get snippet.
     * @param docID int
     * @param queryString String
     * @return String
     */
    private String getSnippet(int docID, String queryString) {
        List<TermPosition> termPositions = Arrays
                .stream(getParsedTerms(new QueryParser("", new EnglishAnalyzer()), queryString))
                .distinct()
                .map(qs -> getSingleTermPositionsSorted(docID, qs, false))
                .flatMap(List::stream)
                .sorted(TermPosition::compareTo)
                .collect(Collectors.toList());

        String content;
        try {
            content = indexSearcher.doc(docID).get("content");
        } catch (IOException e) {
            content = "";
            System.out.println("getSnippet:: Unable to get content");
        }

        // Find a snippet window with highest density
        // Initially, all matched locations are seeds (index, window length).
        // For each seed, it and its n neighbour form a snippet window of n+1 matches
        // The seed with window of smallest length becomes seed for round n+2
        // It keeps iterate until length >= [minSnippetWords]
        if (termPositions.size() > 1) {

            int posStart = 0;
            int posOrder = 0;

            // go higher order and look for better window
            for (int winOrder = 1; winOrder < termPositions.size() - 1; winOrder++) {
                int winLen = Integer.MAX_VALUE;
                int winStart = -1;
                for (int start = 0; start + winOrder < termPositions.size(); start++) {
                    int winDiff = termPositions.get(start + winOrder).position - termPositions.get(start).position + 1;
                    if (winDiff >= minSnippetWords && winDiff <= maxSnippetWords && winDiff < winLen) {
                        winLen = winDiff;
                        winStart = start;
                    }
                }

                int posLen = termPositions.get(posStart + posOrder).position - termPositions.get(posStart).position + 1;
                if (winStart == -1)
                    break;
                if (posLen < minSnippetWords || posLen > maxSnippetWords || (winOrder+1.)/winLen >= (posOrder+1.)/posLen) {
                    posStart = winStart;
                    posOrder = winOrder;
                }
            }

            if (posStart != 0 || posOrder != 0) {
                String snippet = content.substring(termPositions.get(posStart).startOffset, termPositions.get(posStart + posOrder).endOffset);
                System.out.println("getSnippet:: length: " + snippet.length());
                return "... " + snippet + " ...";
            }
        }
        else if (termPositions.size() == 1) {
            content = content.substring(termPositions.get(0).startOffset);
        }
        else {
            System.out.println("getSnippet:: No matched words");
        }
        return Arrays.stream(content.split("((?<=[^a-zA-Z0-9])|(?=[^a-zA-Z0-9]))"))
                .limit(minSnippetWords)
                .collect(Collectors.joining("")) + " ...";
    }

    /**
     * Number of close co-occurrence. This is a score to tell if a returned doc is good. If query
     * words are in close co-occurrence, then there is a higher chance that they are appearing
     * in the document for the same contextual meaning.
     * @param docID int
     * @param queryString String
     * @return int
     */
    private int getNumCloseCoOccurrence(int docID, String queryString) {
        List<TermPosition> termPositions = Arrays
                .stream(getParsedTerms(new QueryParser("", new EnglishAnalyzer()), queryString))
                .distinct()
                .map(qs -> getSingleTermPositionsSorted(docID, qs, true))
                .flatMap(List::stream)
                .sorted(TermPosition::compareTo)
                .collect(Collectors.toList());
        return getCloseCoOccurrenceIndex(termPositions).size();
    }

    /**
     * Perform a lucene search
     * @param queryString String
     * @return SearchedDoc[] an array of SearchedDoc
     * @throws IOException IOException
     * @throws ParseException ParseException
     */
    private SearchedDoc[] luceneSearch(String queryString) throws IOException, ParseException {
        Query query = queryParser.parse(queryString);

        TopDocs topDocs = indexSearcher.search(query, numSearchResult + numExtraSearchResult);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;

        return Arrays.stream(scoreDocs)
                .map((sd) -> new SearchedDoc(String.valueOf(sd.doc), sd.score))
                .toArray(SearchedDoc[]::new);
    }

    /**
     * Perform a hadoop search
     * @param queryString String
     * @return SearchedDoc[] an array of SearchedDoc
     */
    private SearchedDoc[] hadoopSearch(String queryString) {
        String[] tokens = simpleTokenizer(queryString);

        Map<String, Integer> qf = new HashMap<>();
        for (String token: tokens)
            qf.put(token, qf.getOrDefault(token, 0) + 1);

        Map<String, Double> candidate = new HashMap<>();
        qf.forEach((w, f) -> {
            if (hadoopIndex.containsKey(w)) {
                hadoopIndex.get(w).forEach((d, s) ->
                        candidate.put(d,
                                candidate.getOrDefault(d, 0.0) + s * (bm25k2 + 1) * f / (bm25k2 + f)));
            }
        });

        return candidate.entrySet().stream()
                .sorted(Map.Entry.<String, Double> comparingByValue().reversed())
                .limit(numSearchResult + numExtraSearchResult)
                .map(e -> new SearchedDoc(e.getKey(), e.getValue()))
                .toArray(SearchedDoc[]::new);
    }

    @GetMapping("/search")
    public String query(
            @RequestParam(required=false, defaultValue="") String queryString,
            @RequestParam(required=false, defaultValue="") String pDocIDs,
            @RequestParam(defaultValue="lucene") String index
    ) throws IOException, ParseException {

        long startTime = System.nanoTime();

        // if query string is empty, no action is needed
        if (queryString.isEmpty())
            return JSONArray.toJSONString(new ArrayList<>());

        // calculate query centroid by adding query centroid and previous documents' centroids
        String[] previousDocIDs = pDocIDs.split(",");
        double[] queryCentroid = getQueryCentroid(queryString, previousDocIDs);

        // expand query term by word2vec
        if (doQueryExpansion)
            queryString += expandQueryTerms(queryString);

        // Search
        SearchedDoc[] sds;
        if (!index.equals("lucene"))
            sds = hadoopSearch(queryString);
        else
            sds = luceneSearch(queryString);

        // construct return
        List<QueryResult> queryResults = new ArrayList<>();
        for (SearchedDoc sd : sds) {
            String docID = sd.docID;

            Document document = indexSearcher.doc(Integer.parseInt(docID));
            double[] docCentroid = w2vObj.docCentroid.get(docID);

            QueryResult result = new QueryResult();
            result.docID = docID;
            result.url = document.get("url");
            result.title = document.get("title");
            result.previousDocIDs = previousDocIDs;

            result.scoreBM25 = sd.bm25Score;
            result.scorePageRank = Double.parseDouble(document.get("page_rank_score"));
            result.scoreCosineSim = w2vObj.cosSim(docCentroid, queryCentroid);
            result.scoreNumCloseCoOccurrence = getNumCloseCoOccurrence(Integer.parseInt(docID), queryString);
            queryResults.add(result);

            System.out.println(result);
        }

        // sort query result by scores
        sortQueryResult(queryResults, previousDocIDs.length>0);

        // remove extra result
        while (queryResults.size() > numSearchResult)
            queryResults.remove(numSearchResult);

        for (QueryResult qr: queryResults)
            qr.snippet = getSnippet(Integer.parseInt(qr.docID), queryString);

        long endTime = System.nanoTime();
        System.out.println((endTime - startTime)/1e6);

        return JSONArray.toJSONString(queryResults.stream().map(QueryResult::getMap).collect(Collectors.toList()));
    }

    /**
     * Treats only alphanumeric as part of a word, else as delimiter,
     * then change all word to lower case.
     * @param queryString String
     * @return String[] a list of tokens
     */
    private String[] simpleTokenizer(String queryString) {
        return Arrays.stream(queryString.split("[^A-Za-z]"))
                .filter( s -> !s.equals("") && !s.isEmpty() )
                .toArray(String[]::new);
    }

    /**
     * Get a list of index where term position of the index and index - 1 meet with the [closeCoOccurrenceCondition]
     * @param termPositions List<Integer>
     * @return List<Integer>
     */
    private List<Integer> getCloseCoOccurrenceIndex(List<TermPosition> termPositions) {
        List<Integer> ccoIndex = new ArrayList<>();

        int a = -1;
        for (int i = 0; i < termPositions.size(); i++) {
            int b = termPositions.get(i).position;
            if (a >= 0 && b - a < closeCoOccurrenceCondition)
                ccoIndex.add(i);
            a = b;
        }

        return ccoIndex;
    }

    /**
     * query centroid <- normalized(normalized(query centroid) + normalized(previous document's centroid))
     * The idea is that if given a set of previous document IDs indicating user's relevance preference,
     * the query can be modified towards those document. However, since Lucene does not search
     * with embedding, but matching keywords, the biased centroid will be used for ranking only
     * @param queryString String
     * @param previousDocIDs List<String>
     * @return double[] a biased centroid embedding
     */
    private double[] getQueryCentroid(String queryString, String[] previousDocIDs) {
        double[] centroid = new double[w2vObj.embeddingSize];
        Arrays.stream(simpleTokenizer(queryString))
                .filter(s -> w2vObj.w2v.containsKey(s))
                .forEach(s -> {
                    System.out.println("getQueryCentroid::Found " + s);
                    Arrays.setAll(centroid, i -> centroid[i] + w2vObj.w2v.get(s)[i]);
                });
        w2vObj.normalize(centroid);

        if (doQueryModification && previousDocIDs.length > 0) {
            double[] pDocCentroid = new double[w2vObj.embeddingSize];
            Arrays.stream(previousDocIDs)
                    .filter(id -> w2vObj.docCentroid.containsKey(id))
                    .forEach(d -> {
                        System.out.println("getQueryCentroid::Modifying with DocID " + d);
                        Arrays.setAll(pDocCentroid, i -> pDocCentroid[i] + w2vObj.docCentroid.get(d)[i]);
                    });
            w2vObj.normalize(pDocCentroid);

            Arrays.setAll(centroid, i -> centroid[i] + pDocCentroid[i]);
            w2vObj.normalize(centroid);
        }

        return centroid;
    }

    /**
     * Expand query terms with similar words from a predefined set (found by word2vec similarities)
     * @param queryString String
     * @return String
     */
    private String expandQueryTerms(String queryString) {
        StringBuilder newQueryTerm = new StringBuilder();

        Arrays.stream(simpleTokenizer(queryString))
                .filter(qs -> w2vObj.w2w.containsKey(qs))
                .forEach(qs -> {
//                    Collections.shuffle(w2vObj.w2w.get(qs));

                    w2vObj.w2w.get(qs).stream()
                            .filter(ns -> !ns.equals(qs) && isParsable(ns))
                            .limit(queryExpandLimit)
                            .forEach(ns -> {
                                System.out.println("expandQueryTerms:: " + ns);
                                newQueryTerm.append(" ").append(ns);
                            });
                });

        return newQueryTerm.toString();
    }

    /**
     * This produces a ranking where same score receives the same rank. e.g. 1, 2, 2, 3, 4, 5
     * Then it convert rank to score by 1 / rank
     * @param queryResults List<QueryResult>
     */
    private void addScore(List<QueryResult> queryResults, double factor) {
        double rank = 1;
        double last = -1;
        for (QueryResult queryResult : queryResults) {
            queryResult.scoreFinal += factor / rank;
            if (queryResult.scoreFinal != last) {
                last = queryResult.scoreFinal;
                rank++;
            }
        }
    }

    /**
     * This sorts each scoring dimension for which it calculates the rank and final score,
     * all the scores are added together, then the result is sorted along final score and returned
     * @param queryResults List<QueryResult>
     */
    private void sortQueryResult(List<QueryResult> queryResults, boolean hasPDocIDs) {
        queryResults.forEach( r -> r.scoreFinal = 0);

        queryResults.sort(Comparator.comparing(QueryResult::getScoreBM25).reversed());
        addScore(queryResults, 1.0);

        queryResults.sort(Comparator.comparing(QueryResult::getScoreCosineSim).reversed());
        addScore(queryResults, hasPDocIDs ? 10.0 : 1.0);

        queryResults.sort(Comparator.comparing(QueryResult::getScoreNumCloseCoOccurrence).reversed());
        addScore(queryResults, 1.0);

        queryResults.sort(Comparator.comparing(QueryResult::getScorePageRank).reversed());
        addScore(queryResults, 1.0);

        queryResults.sort(Comparator.comparing(QueryResult::getScoreFinal).reversed());
    }
}
