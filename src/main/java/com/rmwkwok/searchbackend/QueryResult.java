package com.rmwkwok.searchbackend;

import java.util.HashMap;
import java.util.Map;

public class QueryResult {
    String docID;
    String url;
    String title;
    String snippet;
    String[] previousDocIDs;

    double scoreFinal;

    double scoreBM25;
    double scorePageRank;
    double scoreCosineSim;
    double scoreNumCloseCoOccurrence;

    Map<String, String> getMap() {
        Map<String, String> map = new HashMap<>();
        map.put("docID", docID);
        map.put("url", url);
        map.put("title", title);
        map.put("snippet", snippet);
        map.put("previousDocIDs", String.join(",", previousDocIDs));
        return map;
    }

    double getScoreBM25() {
        return scoreBM25;
    }

    double getScorePageRank() {
        return scorePageRank;
    }

    double getScoreCosineSim() {
        return scoreCosineSim;
    }

    double getScoreNumCloseCoOccurrence() {
        return scoreNumCloseCoOccurrence;
    }

    double getScoreFinal() {
        return scoreFinal;
    }

    @Override
    public String toString() {
        return "QueryResult{" +
                "docID='" + docID + '\'' +
                ", url='" + url + '\'' +
                ", previousDocIDs=" + previousDocIDs +
                ", scoreFinal=" + scoreFinal +
                ", scoreBM25=" + scoreBM25 +
                ", scorePageRank=" + scorePageRank +
                ", scoreCosineSim=" + scoreCosineSim +
                ", scoreNumCloseCoOccurrence=" + scoreNumCloseCoOccurrence +
                '}';
    }
}
