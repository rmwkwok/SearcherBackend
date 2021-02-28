package com.rmwkwok.searchbackend;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

public class Word2VecObj {

    // A word2vec is downloaded from http://vectors.nlpl.eu/repository/ (ID=5)
    // then processed with a Python script to get the files this class reads
    // Below is a reference to read word2vec with deeplearning4j package.
    // https://deeplearning4j.konduit.ai/language-processing/word2vec#word-2-vec-setup
    // word2Vec = WordVectorSerializer.readWord2VecModel(word2vecPath);

    int embeddingSize;

    String w2vPath = Paths.get(SearchbackendApplication.w2vFolder, "match_words_w2v").toString();
    Map<String, double[]> w2v;

    String docCentroidPath = Paths.get(SearchbackendApplication.w2vFolder, "doc_centroid").toString();
    Map<String, double[]> _docCentroid;
    Map<String, double[]> docCentroid;

    String w2wPath = Paths.get(SearchbackendApplication.w2vFolder, "word_to_word").toString();
    Map<String, List<String>> w2w;

    String d2dPath = Paths.get(SearchbackendApplication.w2vFolder, "doc_to_doc").toString();
    Map<String, List<String>> _d2d;
    Map<String, List<String>> d2d;

    Word2VecObj() throws IOException {

//        IndexReader ir = DirectoryReader.open(FSDirectory.open(Paths.get(SearchbackendApplication.indexFolder)));
//        IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(SearchbackendApplication.indexFolder))));
//        int n=3;
//        Terms tv = ir.getTermVector( n, "content" );
//        TermsEnum terms = tv.iterator();
//        PostingsEnum p = null;
////        PostingsEnum pe = lr.postings(new Term("content", termString));
//        while( terms.next() != null ) {
//            p = terms.postings( p, PostingsEnum.ALL );
//            while( p.nextDoc() != PostingsEnum.NO_MORE_DOCS ) {
//                for( int i = 0; i < p.freq(); i++ ) {
//                    System.out.println( terms.term().utf8ToString() + "," + p.docID() + "," + p.freq() + "," +  p.nextPosition() + "," + p.startOffset() + "," + p.endOffset()); // Fails miserably, of course.
//                }
//            }
//        }
//        Document document = ir.document(n);
//        System.out.println("=============================================");
//        System.out.println(document.get("content"));

//
//        Terms t = reader.getTermVector(1, "content");
//        TermsEnum te = t.iterator();
//
//        BytesRef term = te.next();
//        while (term!=null) {
//            String termString = term.utf8ToString();
//            for (LeafReaderContext lrc : reader.leaves()) {
//                LeafReader lr = lrc.reader();
//                PostingsEnum pe = lr.postings(new Term("content", termString));
//                int docId = pe.nextDoc();
//                while (docId != PostingsEnum.NO_MORE_DOCS) {
//                    System.out.println(termString + "::     " + docId);
//                    docId = pe.nextDoc();
//                }
//                System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
//                System.out.println(term.utf8ToString() + "     ::" + pe.toString());
//                System.out.println(term.utf8ToString() + "     ::" + pe.freq());
//                System.out.println(term.utf8ToString() + "     ::" + pe.startOffset());
//                System.out.println(term.utf8ToString() + "     ::" + pe.endOffset());
//                System.out.println(term.utf8ToString() + "     ::" + pe.getPayload());
//
//                List<Integer> ps = new ArrayList<>();
//                int p = pe.nextPosition();
//                while (p != -1) {
//                    ps.add(p);
//                    p = pe.nextPosition();
//                }
//                System.out.println(term.utf8ToString() + "     ::" + ps);
//            }
//            term = te.next();
//        }

        w2v = readEmbedding(w2vPath);
        _docCentroid = readEmbedding(docCentroidPath);
        w2w = readX2X(w2wPath);
        _d2d = readX2X(d2dPath);
        convertFileNameToDocID();
    }

    public double cosSim(double[] v1, double[] v2) {
        double result = 0;
        for (int i = 0; i < v1.length; i++)
            result += v1[i] * v2[i];
        return result;
    }

    public void normalize(double[] vec) {
        double sqSum = 0;
        for (double i: vec)
            sqSum += i*i;

        sqSum = Math.sqrt(sqSum);
        for (int i = 0; i < vec.length; i++)
            vec[i] /= sqSum;
    }

    private Map<String, double[]> readEmbedding(String path) throws IOException {
        Map<String, double[]> map = new HashMap<>();

        BufferedReader reader = new BufferedReader(new FileReader(path));
        String line = reader.readLine();
        embeddingSize = Integer.parseInt(line.split(" ")[1]);

        line = reader.readLine();
        while (line != null) {
            double[] embedding = new double[embeddingSize];
            String[] s = line.split(" ");

            Arrays.setAll(embedding, (i) -> Double.parseDouble(s[i+1]));
            map.put(s[0], embedding);

            line = reader.readLine();
        }
        reader.close();

        System.out.println(path + ":::::" + map.size());
        return map;
    }

    private Map<String, List<String>> readX2X(String path) throws IOException {
        Map<String, List<String>>  x2x = new HashMap<>();

        BufferedReader reader = new BufferedReader(new FileReader(path));
        String line;

        line = reader.readLine();
        line = reader.readLine();
        while (line != null) {
            String[] x = line.split(" ");

            List<String> x2 = new ArrayList<>(Arrays.asList(x).subList(1, x.length));
            x2x.put(x[0], x2);

            line = reader.readLine();
        }
        reader.close();

        System.out.println(path + ":::::" + x2x.size());
        return x2x;
    }

    private void convertFileNameToDocID() throws IOException {
        if (_d2d == null)
            return;
        else {
            d2d = new HashMap<>();
            docCentroid = new HashMap<>();
        }

        Map<String, String> mapping = new HashMap<>();
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(SearchbackendApplication.indexFolder)));
        for (int i = 0; i < reader.maxDoc(); i++)
            mapping.put(reader.document(i).get("filename"), String.valueOf(i));
        reader.close();

        _d2d.keySet().forEach(key -> {
            if (!mapping.containsKey(key))
                return;

            List<String> temp = new ArrayList<>();
            _d2d.get(key).forEach(val -> {
                if (!mapping.containsKey(val))
                    return;
                temp.add(mapping.get(val));
            });
            d2d.put(mapping.get(key), temp);
        });

        _docCentroid.keySet().forEach(key -> {
            if (!mapping.containsKey(key))
                return;
            docCentroid.put(mapping.get(key), _docCentroid.get(key));
        });

        _d2d.clear();
        _docCentroid.clear();
    }
}
