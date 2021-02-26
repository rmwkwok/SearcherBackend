//package com.rmwkwok.searchbackend;
//
//import org.apache.lucene.analysis.TokenFilter;
//import org.apache.lucene.analysis.TokenStream;
//import org.apache.lucene.analysis.synonym.SynonymFilter;
//import org.apache.lucene.util.CharsRef;
//import org.apache.lucene.util.CharsRefBuilder;
//import org.deeplearning4j.models.word2vec.Word2Vec;
//
//import java.io.IOException;
//import java.util.List;
//
//public class W2VSynonymFilter extends TokenFilter {
//
//    Word2Vec word2Vec;
//    List<PendingOutput> outputs;
//
//    protected W2VSynonymFilter(TokenStream input, Word2Vec word2Vec) {
//        super(input);
//        this.word2Vec = word2Vec;
//    }
//
//    @Override
//    public boolean incrementToken() throws IOException {
//        if (!outputs.isEmpty()) {
//            PendingOutput output = outputs.remove(0);
//
//            restoreState(output.state);
//
//            termAtt.copyBuffer(output.charsRef.chars,
//
//                    output.charsRef.offset, output.charsRef.length);
//
//            typeAtt.setType(SynonymFilter.TYPE_SYNONYM);
//            return true;
//        }
//
//        if (!SynonymFilter.TYPE_SYNONYM.equals(typeAtt.type())) {
//            String word = new String(termAtt.buffer()).trim();
//
//            List<String> list = word2Vec.similarWordsInVocabTo(word, minAccuracy);
//            int i = 0;
//            for (String syn : list) {
//
//                if (i == 2) {
//                    break;
//                }
//                if (!syn.equals(word)) {
//                    CharsRefBuilder charsRefBuilder = new CharsRefBuilder();
//
//                    CharsRef cr = charsRefBuilder.append(syn).get();
//
//
//                    State state = captureState();
//
//                    outputs.add(new PendingOutput(state, cr));
//                    i++;
//                }
//            }
//        }
//        return !outputs.isEmpty() || input.incrementToken();
//    }
//
//}
//
