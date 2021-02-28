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

import java.io.IOException;
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

    Search() throws IOException {
        long startTime = System.nanoTime();

        // Lucene
        queryParser = new MultiFieldQueryParser(new String[] {"content", "title", "anchor_text"}, new EnglishAnalyzer());
        indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(SearchbackendApplication.indexFolder)));
        indexSearcher = new IndexSearcher(indexReader);
        indexSearcher.setSimilarity(new BM25Similarity());

        // Common
        w2vObj = new Word2VecObj();

        long endTime = System.nanoTime();
        System.out.println("Search initialization took " + (endTime - startTime)/1e6 + "ms");
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
        if (termPositions.size() > 0) {
            int n = 0;
            Map<Integer, Integer> seed = new HashMap<>();
            for (int i = 0; i < termPositions.size(); i++)
                seed.put(i, 1);

            int minLength;
            do {
                n++;
                Map<Integer, Integer> temp = new HashMap<>();

                for (Map.Entry<Integer, Integer> entry : seed.entrySet())
                    for (int index : new int[]{entry.getKey() - 1, entry.getKey()})
                        if ((index >= 0) && (index + n < termPositions.size()))
                            temp.put(index, termPositions.get(index + n).position - termPositions.get(index).position + 1);

                seed.clear();

                minLength = temp.values().stream().min(Integer::compare).orElse(minSnippetWords);
                for (Map.Entry<Integer, Integer> entry : temp.entrySet())
                    if (entry.getValue() == minLength)
                        seed.put(entry.getKey(), entry.getValue());

            } while (minLength < minSnippetWords);

            // i and j are beginning and ending index for matchedLocations
            // need them to get index of contentTerms
            int i = seed.keySet().stream().min(Integer::compare).orElse(0);
            int j = Math.min(i + n, termPositions.size() - 1);

            while (termPositions.get(j).position - termPositions.get(i).position >= maxSnippetWords)
                j-=1;

            String snippet = content.substring(termPositions.get(i).startOffset, termPositions.get(j).endOffset);
            System.out.println("getSnippet:: length: " + snippet.length());
            return "... " + snippet + " ...";
        }
        else {
            System.out.println("getSnippet:: No matched words");
            return String.join("",
                    Arrays.stream(content.split("((?<=[^a-zA-Z0-9])|(?=[^a-zA-Z0-9]))"))
                            .limit(minSnippetWords)
                            .collect(Collectors.toList())) + " ...";
        }
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

    @GetMapping("/lucene")
    public String query(
            @RequestParam(required=false, defaultValue="") String queryString,
            @RequestParam(required=false, defaultValue="") String pDocIDs
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

        // Lucene search
        Query query = queryParser.parse(queryString);

        TopDocs topDocs = indexSearcher.search(query, numSearchResult + numExtraSearchResult);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;

        // construct return
        List<QueryResult> queryResults = new ArrayList<>();
        for (ScoreDoc scoreDoc : scoreDocs) {
            int docID = scoreDoc.doc;

            Document document = indexSearcher.doc(docID);
            double[] docCentroid = w2vObj.docCentroid.get(String.valueOf(docID));

            QueryResult result = new QueryResult();
            result.docID = String.valueOf(docID);
            result.url = document.get("url");
            result.title = document.get("title");
            result.previousDocIDs = previousDocIDs;

            result.scoreBM25 = scoreDoc.score;
            result.scorePageRank = Double.parseDouble(document.get("page_rank_score"));
            result.scoreCosineSim = w2vObj.cosSim(docCentroid, queryCentroid);
            result.scoreNumCloseCoOccurrence = getNumCloseCoOccurrence(docID, queryString);
            queryResults.add(result);

            System.out.println(result);
        }

        // sort query result by scores
        sortQueryResult(queryResults);

        // remove extra result
        while (queryResults.size() > numSearchResult)
            queryResults.remove(numSearchResult);

        for (QueryResult qr: queryResults)
            qr.snippet = getSnippet(Integer.valueOf(qr.docID), queryString);

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
        return Arrays.stream(queryString.split("[^A-Za-z0-9]"))
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
                    Collections.shuffle(w2vObj.w2w.get(qs));

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
    private void addScore(List<QueryResult> queryResults) {
        double rank = 1;
        double last = -1;
        for (QueryResult queryResult : queryResults) {
            queryResult.scoreFinal += 1. / rank;
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
    private void sortQueryResult(List<QueryResult> queryResults) {
        queryResults.forEach( r -> r.scoreFinal = 0);

        queryResults.sort(Comparator.comparing(QueryResult::getScoreBM25).reversed());
        addScore(queryResults);

        queryResults.sort(Comparator.comparing(QueryResult::getScoreCosineSim).reversed());
        addScore(queryResults);

        queryResults.sort(Comparator.comparing(QueryResult::getScoreNumCloseCoOccurrence).reversed());
        addScore(queryResults);

        queryResults.sort(Comparator.comparing(QueryResult::getScorePageRank).reversed());
        addScore(queryResults);

        queryResults.sort(Comparator.comparing(QueryResult::getScoreFinal).reversed());
    }
}
