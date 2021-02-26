package com.rmwkwok.searchbackend;


import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.json.simple.JSONArray;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

@RestController
@RequestMapping("/search")
@CrossOrigin("*")
public class LuceneSearch {

    // configuration
    final int numSearchResult = 25;
    final int minSnippetWords = 30*2;
    final private String indexPath = "/home/raymondkwok/git/InformationRetrieval/index";
    // configuration end

    final private QueryParser queryParser;
    final private QueryParser snippetParser;
    final private IndexSearcher indexSearcher;

//        final private CustomAnalyzer customAnalyzer = CustomAnalyzer.builder()
//            .withTokenizer(StandardTokenizerFactory.NAME)
//            .addTokenFilter("englishPossessive")
//            .addTokenFilter("lowercase")
//            .addTokenFilter("stop")
//            .addTokenFilter("porterStem")
//            .addTokenFilter(W2VSynonymFilter)

    LuceneSearch() throws IOException {
        queryParser = new QueryParser("content", new EnglishAnalyzer());
        snippetParser = new QueryParser("", new StandardAnalyzer());
        indexSearcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath))));
//        indexSearcher.setSimilarity(new BM25Similarity());
    }

    private TopDocs searchTopDocs(String queryString)
            throws IOException, ParseException {
        Query query = queryParser.parse(queryString);
        return indexSearcher.search(query, numSearchResult);
    }

    private Document getDocument(int docId)
            throws IOException {
        return indexSearcher.doc(docId);
    }

    private String[] getParsedTerms(String queryString) {

        // use the snippetParser.parse to do the parsing
        // however, I do not know how to get parsed terms properly
        // so use toString() and expect a list of field:term delimited by space
        try {
            return snippetParser.parse(queryString).toString().split(" ");
        }
        catch (ParseException e) {
            return new String[0];
        }
    }

    private String[] getDistinctTerms(String[] strings) {
        return Arrays.stream(strings).distinct().toArray(String[]::new);
    }

    private String getSnippet(String queryString, String contentString) {

        // convert queryString to a list of parsed words
        String[] queryTerms = getParsedTerms(queryString);

        // keep distinct query terms only
        queryTerms = getDistinctTerms(queryTerms);

        // convert contentTerms to List of terms
        String[] contentTerms = contentString.split("((?<=[^a-zA-Z0-9])|(?=[^a-zA-Z0-9]))");

        // snippet extraction
        // First, locate all matched terms.
        List<Integer> matchedLocations = new ArrayList<>();

        for (int i = 0; i < contentTerms.length; i++) {
            boolean isMatch = false;
            // parse contentTerm and compare each with query Term
            // a contentTerm can contain more than one sub-terms,
            // as soon as a sub-term matches with a query term,
            // the whole contentTerm is considered matched
            for (String content : getParsedTerms(contentTerms[i])) {
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

        return String.join("", Arrays.copyOfRange(contentTerms, matchedLocations.get(i), matchedLocations.get(j)));
    }

    @GetMapping("/lucene")
    public String query(
            @RequestParam(required=false, defaultValue="") String queryString
    ) throws IOException, ParseException {

        long startTime = System.nanoTime();

        List<Map<String, String>> queryResults = new ArrayList<>();
        TopDocs topDocs = searchTopDocs(queryString);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;

        for (ScoreDoc scoreDoc : scoreDocs) {
            Document document = getDocument(scoreDoc.doc);

            Map<String, String> result = new HashMap<>();
            result.put("age", document.get("Headers.Age"));
            result.put("url", document.get("url"));
            result.put("title", document.get("title"));
            result.put("page_rank_score", document.get("page_rank_score"));
            result.put("snippet", getSnippet(queryString, document.get("content")));
            queryResults.add(result);
        }

        long endTime = System.nanoTime();
        System.out.println((endTime - startTime)/1e6);

        return JSONArray.toJSONString(queryResults);
    }
}
