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
//import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
//import org.deeplearning4j.models.word2vec.Word2Vec;
import org.json.simple.JSONArray;
import org.springframework.web.bind.annotation.*;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

@RestController
@RequestMapping("/search")
@CrossOrigin("*")
public class LuceneSearch {

    // configuration
//    final boolean doQueryExpansion = false;
    final private int closeCoOccurrenceCondition = SearchbackendApplication.closeCoOccurrenceCondition;
    final private int numSearchResult = SearchbackendApplication.numSearchResult;
    final private int minSnippetWords = SearchbackendApplication.minSnippetWords;
    final private String indexPath = SearchbackendApplication.indexPath;
//    final private String docCentroidPath = "/home/raymondkwok/git/InformationRetrieval/docCentroid.txt";

    // ID = 5 is used. http://vectors.nlpl.eu/repository/
//    final private String word2vecPath = "/home/raymondkwok/git/InformationRetrieval/word2vec/model.bin";
    // configuration end

//    final private Word2Vec word2Vec;
    final private QueryParser queryParser;
    final private QueryParser snippetParser;
    final private IndexSearcher indexSearcher;

    LuceneSearch() throws IOException {

        long startTime = System.nanoTime();
        queryParser = new QueryParser("content", new EnglishAnalyzer());
        snippetParser = new QueryParser("", new StandardAnalyzer());
        indexSearcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath))));
        indexSearcher.setSimilarity(new BM25Similarity());

        // https://deeplearning4j.konduit.ai/language-processing/word2vec#word-2-vec-setup
//        word2Vec = WordVectorSerializer.readWord2VecModel(word2vecPath);

//        if (!Files.exists(Paths.get(docCentroidPath))) {
//            getDocCentroid();
//        }

        long endTime = System.nanoTime();
        System.out.println((endTime - startTime)/1e6);
    }

//    private void getDocCentroid() throws IOException {
//        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
//        int numDoc = reader.maxDoc();
//
//        FileWriter myWriter = new FileWriter(docCentroidPath);
//        myWriter.write(String.valueOf(numDoc) + " " + String.valueOf(word2Vec.getLayerSize()) + "\n");
//
//        for (int docID = 0; docID < numDoc; docID++) {
//            int vectorCount = 0;
//            double[] vectorSum = new double[word2Vec.getLayerSize()];
//            for (String term: reader.document(docID).get("content").split("[^a-zA-Z0-9]")) {
//                double[] vector = word2Vec.getWordVector(term);
//                if (vector != null) {
//                    Arrays.setAll(vectorSum, i -> vectorSum[i] + vector[i]);
//                    vectorCount++;
//                }
//            }
//
//            myWriter.write(String.valueOf(docID));
//            for (double v: vectorSum)
//                myWriter.write(" " + String.valueOf(v / vectorCount));
//            myWriter.write('\n');
//        }
//        myWriter.close();
//    }

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

        try {
            return String.join("", Arrays.copyOfRange(contentTerms, matchedLocations.get(i), matchedLocations.get(j)));
        } catch (Exception e) {
            return "No Snippet Available";
        }
    }

//    private String expandQueryTerms(String queryString) {
//
//        for (String term: getParsedTerms(snippetParser, queryString)) {
//            List<String> newTerms = word2Vec.similarWordsInVocabTo(term, 0.8);
//
//            int count = 0;
//            for (String newTerm: newTerms) {
//                if (count < 3 && !newTerm.equals(term)) {
//                    try {
//                        queryParser.parse(newTerm);
//                    } catch (ParseException e) {
//                        continue;
//                    }
//                    queryString = queryString + " " + newTerm;
//                    count++;
//                }
//            }
//        }
//        return queryString;
//    }

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

    @GetMapping("/lucene")
    public String query(
            @RequestParam(required=false, defaultValue="") String queryString
    ) throws IOException, ParseException {

        long startTime = System.nanoTime();

        // expand query term by word2vec
//        if (doQueryExpansion)
//            queryString = expandQueryTerms(queryString);

        List<Map<String, Object>> queryResults = new ArrayList<>();

        // Lucene search
        Query query = queryParser.parse(queryString);
        TopDocs topDocs = indexSearcher.search(query, numSearchResult);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;

        //test
//        FSDirectory directory = FSDirectory.open(Paths.get(indexPath));
//        IndexReader reader = DirectoryReader.open(directory);

        // construct return
        for (ScoreDoc scoreDoc : scoreDocs) {
            Document document = indexSearcher.doc(scoreDoc.doc);

//            Terms terms = reader.getTermVector(scoreDoc.doc, "content");
//            final TermsEnum it = terms.iterator();
//            BytesRef term = it.next();
//            while (term != null) {
//                String termString = term.utf8ToString();
//                System.out.print(termString + ": ");
//                term = it.next();
//            }

            Map<String, Object> result = new HashMap<>();
            result.put("age", document.get("Headers.Age"));
            result.put("url", document.get("url"));
            result.put("title", document.get("title"));
            result.put("page_rank_score", Double.parseDouble(document.get("page_rank_score")));
            result.put("BM25_score", scoreDoc.score);
            result.put("numCloseCoOccurrence", getNumCloseCoOccurrence(queryString, document.get("content")));
            result.put("snippet", getSnippet(queryString, document.get("content")));
            queryResults.add(result);
        }

        long endTime = System.nanoTime();
        System.out.println((endTime - startTime)/1e6);

        return JSONArray.toJSONString(queryResults);
    }
}
