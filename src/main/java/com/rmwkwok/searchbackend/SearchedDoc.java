package com.rmwkwok.searchbackend;

public class SearchedDoc {
    final String docID;
    final double bm25Score;

    SearchedDoc(String docID, double bm25Score) {
        this.docID = docID;
        this.bm25Score = bm25Score;
    }
}
