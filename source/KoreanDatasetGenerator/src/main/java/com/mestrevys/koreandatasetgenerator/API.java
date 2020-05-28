/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mestrevys.koreandatasetgenerator;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import kr.bydelta.koala.data.Morpheme;
import kr.bydelta.koala.data.Sentence;
import kr.bydelta.koala.data.Word;
import kr.bydelta.koala.eunjeon.Tagger;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author andersoncarlosfs
 */
public class API {

    /**
     * 
     * @param args the command line arguments
     * @throws java.lang.InterruptedException
     * @throws java.util.concurrent.ExecutionException
     */
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        //
        int threads = 16 * Runtime.getRuntime().availableProcessors();

        //
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        //
        Tagger tagger = new Tagger();

        //
        AtomicLong limit = new AtomicLong(10000000);

        //
        AtomicLong next = new AtomicLong(0);

        //
        LinkedList<Future<?>> futures = new LinkedList<>();
        
        //
        long stamps = 0;
        
        //
        Instant latest = Instant.now();
        
        //
        while ((limit.get() > 0) && ((stamps < 1000) || (ChronoUnit.HOURS.between(latest, Instant.now()) > 1))) {
            //
            long current = limit.get();
            
            //
            for (; threads > 0; threads--) {
                futures.add(executor.submit(() -> {

                    try {
                        //
                        Collection<String> texts = new LinkedList<>();

                        //
                        URL movies = new URL("https://api.themoviedb.org/3/movie/" + next.incrementAndGet() + "?api_key=fbecd78b5e77c89614e292cb5f7b1f89&language=ko-KO");

                        //
                        texts.add(API.getData(movies).optString("overview"));

                        //
                        URL series = new URL("https://api.themoviedb.org/3/tv/" + next.incrementAndGet() + "?api_key=fbecd78b5e77c89614e292cb5f7b1f89&language=ko-KO");

                        //
                        JSONObject serie = API.getData(series);

                        //
                        for (int i = 0; i < serie.optLong("number_of_seasons"); i++) {
                            //
                            URL seasons = new URL("https://api.themoviedb.org/3/tv/" + next.incrementAndGet() + "/season/" + i + "?api_key=fbecd78b5e77c89614e292cb5f7b1f89&language=ko-KO");

                            //
                            JSONObject season = API.getData(seasons);

                            //
                            JSONArray episodes = season.optJSONArray("episodes");

                            //
                            for (int j = 0; ((episodes != null) && (j < episodes.length())); j++) {
                                //
                                texts.add(episodes.getJSONObject(j).optString("overview"));
                            }

                            //
                            texts.add(season.optString("overview"));
                        }

                        //
                        texts.add(serie.optString("overview"));

                        //
                        texts.removeIf(item -> item == null || item.trim().isBlank());

                        //
                        for (String text : texts) {
                            //
                            for (Sentence sentence : tagger.tag(text)) {
                                //
                                for (Word word : sentence) {
                                    //
                                    limit.decrementAndGet();
                    
                                    for (Morpheme morpheme : word) {
                                        System.out.println(word.getSurface() + " " + morpheme.getSurface() + " " + morpheme.getTag());
                                        //break;
                                    }
                                }
                                //break;
                            }
                        }
                    } catch (MalformedURLException exception) {
                        exception.printStackTrace();
                    }
                    //break;
                }));
            }

            //
            for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
                futures.get(i).get();
            }
            
            //
            for (int i = 0; i < futures.size(); i++) {
                //
                if(futures.get(i).isDone()) {                    
                    //
                    futures.remove(i);
                    
                    //
                    threads++;
                }
            }
            
            //
            if (current == limit.get()) {
                stamps++;
            } else {
                //
                stamps = 0;
                
                //
                latest = Instant.now();
            }
            
        }

        //
        executor.shutdown();
    }

    private static JSONObject getData(URL url) {
        String string = "{}";
        try (InputStream input = url.openStream()) {
            string = IOUtils.toString(input);
        } catch (IOException exception) {
            exception.printStackTrace();
        } finally {
            return new JSONObject(string);
        }
    }
    
}
