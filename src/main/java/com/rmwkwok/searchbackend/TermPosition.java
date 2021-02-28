package com.rmwkwok.searchbackend;

public class TermPosition {
    String term;
    int position;
    int startOffset;
    int endOffset;

    TermPosition(String term, int position, int startOffset, int endOffset) {
        this.term = term;
        this.position = position;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public int compareTo(TermPosition other) {
        return Integer.compare(this.position, other.position);
    }
}
