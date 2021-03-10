package com.rmwkwok.searchbackend;

public class SearchedDoc {
    final int docID;
    final float bm25Score;

    SearchedDoc(int docID, float bm25Score) {
        this.docID = docID;
        this.bm25Score = bm25Score;
    }
}
