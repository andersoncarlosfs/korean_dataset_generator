/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mestrevys.koreandatasetgenerator;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import kr.bydelta.koala.data.Morpheme;
import kr.bydelta.koala.data.Sentence;
import kr.bydelta.koala.data.Word;
import kr.bydelta.koala.eunjeon.Tagger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

/**
 *
 * @author andersoncarlosfs
 */
public class TXT {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        //
        try (Reader reader = new InputStreamReader(TXT.class.getClassLoader().getResourceAsStream("movies.txt"))) {
            //
            int threads = 16 * Runtime.getRuntime().availableProcessors();

            //
            ExecutorService executor = Executors.newFixedThreadPool(threads);

            //
            Tagger tagger = new Tagger();
           
            //
            for (CSVRecord record : CSVFormat.TDF.parse(reader)) {
                executor.execute(() -> {
                    //
                    for (Sentence sentence : tagger.tag(record.get(1))) {
                        //
                        for (Word word : sentence) {
                            //
                            for (Morpheme morpheme : word) {
                                //
                                System.out.println(word.getSurface() + " " + morpheme.getSurface() + " " + morpheme.getTag());
                                //break;
                            }
                        }
                        //break;
                    }
                    //break;
                });
                //break;
            }

            //
            executor.shutdown();
        }
    }

}
