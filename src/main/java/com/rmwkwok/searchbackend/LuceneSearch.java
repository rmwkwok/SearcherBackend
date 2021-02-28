package com.rmwkwok.searchbackend;

import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
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
public class LuceneSearch {

    final boolean doQueryExpansion = SearchbackendApplication.doQueryExpansion;
    final boolean doQueryModification = SearchbackendApplication.doQueryModification;
    final private int minSnippetWords = SearchbackendApplication.minSnippetWords;
    final private int numSearchResult = SearchbackendApplication.numSearchResult;
    final private int numExtraSearchResult = SearchbackendApplication.numExtraSearchResult;
    final private int closeCoOccurrenceCondition = SearchbackendApplication.closeCoOccurrenceCondition;

    final private Word2VecObj w2vObj;
    final private QueryParser queryParser;
    final private QueryParser snippetParser;
    final private IndexSearcher indexSearcher;

    LuceneSearch() throws IOException {
        long startTime = System.nanoTime();
        queryParser = new QueryParser("content", new EnglishAnalyzer());
        snippetParser = new QueryParser("", new StandardAnalyzer());
        indexSearcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(SearchbackendApplication.indexFolder))));
        indexSearcher.setSimilarity(new BM25Similarity());
        w2vObj = new Word2VecObj();
        long endTime = System.nanoTime();
        System.out.println("LuceneSearch initialization took " + (endTime - startTime)/1e6 + "ms");
    }

    private String[] getParsedTerms(QueryParser queryParser, String queryString) {
        // use the snippetParser.parse to do the parsing
        // however, I do not know how to get parsed terms properly
        // so use toString() and expect a list of field:term delimited by space
        try {
            return queryParser.parse(queryString).toString().split(" ");
        }
        catch (ParseException e) {
            return new String[0];
        }
    }

    private String[] getDistinctTerms(String[] strings) {
        return Arrays.stream(strings).distinct().toArray(String[]::new);
    }

    private List<Integer> getMatchedLocations(String[] queryTerms, String[] contentTerms) {
        List<Integer> matchedLocations = new ArrayList<>();

        for (int i = 0; i < contentTerms.length; i++) {
            boolean isMatch = false;
            // parse contentTerm and compare each with query Term
            // a contentTerm can contain more than one sub-terms,
            // as soon as a sub-term matches with a query term,
            // the whole contentTerm is considered matched
            for (String content : getParsedTerms(snippetParser, contentTerms[i])) {
                for (String query : queryTerms) {
                    if (content.equals(query)) {
                        matchedLocations.add(i);
                        isMatch = true;
                        break;
                    }
                }
                if (isMatch) break;
            }
        }

        return matchedLocations;
    }

    private double[] getQueryCentroid(String queryString, List<String> previousDocIDs) {
        double[] centroid = new double[w2vObj.embeddingSize];
        for (String term: getParsedTerms(snippetParser, queryString)) {
            if (w2vObj.w2v.containsKey(term)) {
                Arrays.setAll(centroid, (i) -> centroid[i] + w2vObj.w2v.get(term)[i]);
            }
        }
        w2vObj.normalize(centroid);

        if (doQueryModification && previousDocIDs.size() > 0) {
            double[] pDocCentroid = new double[w2vObj.embeddingSize];
            for (String docID : previousDocIDs) {
                if (w2vObj.docCentroid.containsKey(docID)) {
                    System.out.println("Modifying query centroid with DocID " + docID);
                    Arrays.setAll(pDocCentroid, (i) -> pDocCentroid[i] + w2vObj.docCentroid.get(docID)[i]);
                }
            }
            w2vObj.normalize(pDocCentroid);

            Arrays.setAll(centroid, (i) -> centroid[i] + pDocCentroid[i]);
            w2vObj.normalize(centroid);
        }

        return centroid;
    }

    private String expandQueryTerms(String queryString) {
        StringBuilder newQueryTerm = new StringBuilder();
        for (String term: getParsedTerms(snippetParser, queryString)) {
            if (w2vObj.w2w.containsKey(term)) {
                Collections.shuffle(w2vObj.w2w.get(term));

                int count = 0;
                for (String newTerm : w2vObj.w2w.get(term)) {
                    if (count < 3 && !newTerm.equals(term)) {
                        try {
                            queryParser.parse(newTerm);
                        } catch (ParseException e) {
                            continue;
                        }
                        System.out.println("Expanding query with " + newTerm);
                        newQueryTerm.append(" ").append(newTerm);
                        count++;
                    }
                }
            }
        }
        return newQueryTerm.toString();
    }

    private String getSnippet(String queryString, String contentString) {

        // convert queryString to a list of parsed words
        String[] queryTerms = getParsedTerms(snippetParser, queryString);

        // keep distinct query terms only
        queryTerms = getDistinctTerms(queryTerms);

        // convert contentTerms to List of terms
        String[] contentTerms = contentString.split("((?<=[^a-zA-Z0-9])|(?=[^a-zA-Z0-9]))");

        // snippet extraction
        // First, locate all matched terms.
        List<Integer> matchedLocations = getMatchedLocations(queryTerms, contentTerms);

        // Second, find a snippet window with highest density
        // Initially, all matched locations are seeds (index, window length).
        // For each seed, it and its n neighbour form a snippet window of n+1 matches
        // The seed with window of smallest length becomes seed for round n+2
        // It keeps iterate until length >= [minSnippetWords]
        int n = 0;
        Map<Integer, Integer> seed = new HashMap<>();
        for (int i = 0; i < matchedLocations.size(); i++)
            seed.put(i, 1);

        int minLength;
        do {
            n++;
            Map<Integer, Integer> temp = new HashMap<>();

            for (Map.Entry<Integer, Integer> entry : seed.entrySet())
                for (int index: new int[] {entry.getKey() - 1, entry.getKey()})
                    if ((index >= 0) && (index + n < matchedLocations.size()))
                        temp.put(index, matchedLocations.get(index + n) - matchedLocations.get(index) + 1);

            seed.clear();

            minLength = temp.values().stream().min(Integer::compare).orElse(minSnippetWords);
            for (Map.Entry<Integer, Integer> entry : temp.entrySet())
                if (entry.getValue() == minLength)
                    seed.put(entry.getKey(), entry.getValue());

        } while (minLength < minSnippetWords);

        // i and j are beginning and ending index for matchedLocations
        // need them to get index of contentTerms
        int i = seed.keySet().stream().min(Integer::compare).orElse(0);
        int j = i + n;

        if (i < 0)
            i = 0;
        if (j < 0)
            j = 0;
        if (i >= matchedLocations.size())
            i = matchedLocations.size() - 1;
        if (j >= matchedLocations.size())
            j = matchedLocations.size() - 1;

        try {
            return String.join("", Arrays.copyOfRange(contentTerms, matchedLocations.get(i), matchedLocations.get(j)));
        } catch (Exception e) {
            return "Snippet Not Available";
        }
    }

    private int getNumCloseCoOccurrence(String queryString, String contentString) {
        // convert queryString to a list of parsed words
        String[] queryTerms = getParsedTerms(snippetParser, queryString);

        // keep distinct query terms only
        queryTerms = getDistinctTerms(queryTerms);

        // convert contentTerms to List of terms
        String[] contentTerms = contentString.split("[^a-zA-Z0-9]");

        // snippet extraction
        // First, locate all matched terms.
        List<Integer> matchedLocations = getMatchedLocations(queryTerms, contentTerms);

        int numCloseCoOccurrence = 0;

        for (int i = 0; i < matchedLocations.size() - 1; i++) {
            int loc1 = matchedLocations.get(i+1);
            int loc2 = matchedLocations.get(i);
            if (loc1 - loc2 <= closeCoOccurrenceCondition && !contentTerms[loc1].equals(contentTerms[loc2]))
                numCloseCoOccurrence += 1;
        }

        return numCloseCoOccurrence;
    }

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

    @GetMapping("/lucene")
    public String query(
            @RequestParam(required=false, defaultValue="") String queryString,
            @RequestParam(required=false, defaultValue="") String docID
    ) throws IOException, ParseException {

        if (queryString.isEmpty())
            return JSONArray.toJSONString(new ArrayList<>());

        long startTime = System.nanoTime();

        List<String> previousDocIDs = new ArrayList<>();
        List<QueryResult> queryResults = new ArrayList<>();

        if (!docID.isEmpty())
            Collections.addAll(previousDocIDs, docID.split(","));

        // get queryCentroid before expanding
        double[] queryCentroid = getQueryCentroid(queryString, previousDocIDs);

        // expand query term by word2vec
        if (doQueryExpansion)
            queryString += expandQueryTerms(queryString);

        // Lucene search
        Query query = queryParser.parse(queryString);
        TopDocs topDocs = indexSearcher.search(query, numSearchResult + numExtraSearchResult);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;

        // construct return
        for (ScoreDoc scoreDoc : scoreDocs) {
            Document document = indexSearcher.doc(scoreDoc.doc);
            double[] docCentroid = w2vObj.docCentroid.get(String.valueOf(scoreDoc.doc));

            QueryResult result = new QueryResult();
            result.docID = String.valueOf(scoreDoc.doc);
            result.url = document.get("url");
            result.title = document.get("title");
            result.snippet = getSnippet(queryString, document.get("content"));
            result.previousDocIDs = previousDocIDs;

            result.scoreBM25 = scoreDoc.score;
            result.scorePageRank = Double.parseDouble(document.get("page_rank_score"));
            result.scoreCosineSim = w2vObj.cosSim(docCentroid, queryCentroid);
            result.scoreNumCloseCoOccurrence = getNumCloseCoOccurrence(queryString, document.get("content"));
            queryResults.add(result);

            System.out.println(result);
        }

        sortQueryResult(queryResults);

        while (queryResults.size() > numSearchResult)
            queryResults.remove(numSearchResult);

        long endTime = System.nanoTime();
        System.out.println((endTime - startTime)/1e6);

        return JSONArray.toJSONString(
                queryResults
                        .stream()
                        .map(QueryResult::getMap)
                        .collect(Collectors.toList()));
    }
}
