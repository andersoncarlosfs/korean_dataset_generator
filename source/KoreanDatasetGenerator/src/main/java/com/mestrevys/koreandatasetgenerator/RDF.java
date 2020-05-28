/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mestrevys.koreandatasetgenerator;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import kr.bydelta.koala.data.Morpheme;
import kr.bydelta.koala.data.Sentence;
import kr.bydelta.koala.data.Word;
import kr.bydelta.koala.eunjeon.Tagger;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;

/**
 *
 * @author andersoncarlosfs
 */
public class RDF {
   
    /**
     *
     */
    public static class PropertyFilterSinkRDF extends PipedTriplesStream {

        /**
         *
         */
        private final List<Node> properties;

        /**
         *
         * @param sink
         * @param properties
         */
        private PropertyFilterSinkRDF(PipedRDFIterator<Triple> sink, Collection<Property> properties) {
            //
            super(sink);
            
            //
            this.properties = properties.stream().parallel().map(Property::asNode).collect(Collectors.toCollection(LinkedList::new));            
        }

        /**
         *
         * @param triple
         */
        @Override
        public void triple(Triple triple) {                 
            if (this.properties.contains(triple.getPredicate())) {
                super.triple(triple);
            }
        }

    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws MalformedURLException, FileNotFoundException, IOException {                                              
        //
        URL url = new URL("https://downloads.dbpedia.org/repo/dbpedia/text/long-abstracts/2020.02.01/long-abstracts_lang%3dko.ttl.bz2");
        
        //
        try (FileOutputStream output = new FileOutputStream("kway.bz2")) {
            //
            output.getChannel().transferFrom(Channels.newChannel(url.openStream()), 0, Long.MAX_VALUE);
            //
            output.flush();
            //
        }              
        
        //
        try (OutputStream output = Files.newOutputStream(Paths.get("kway.ttl")); BZip2CompressorInputStream input = new BZip2CompressorInputStream(new BufferedInputStream(Files.newInputStream(Paths.get("kway.bz2"))))) {
            //
            final byte[] buffer = new byte[8192];
            
            int n = 0;
            
            while (-1 != (n = input.read(buffer))) {
                output.write(buffer, 0, n);
            }
            
            //
            output.flush(); 
        } 
        
        //
        PipedRDFIterator<Triple> iterator = new PipedRDFIterator<>();
                      
        //
        final PipedRDFStream<Triple> stream = new PropertyFilterSinkRDF(iterator, Arrays.asList(ResourceFactory.createProperty("http://dbpedia.org/ontology/abstract")));
        
        // PipedRDFStream and PipedRDFIterator need to be on different threads
        ExecutorService executor = Executors.newSingleThreadExecutor();

        // Start the parser on another thread
        executor.execute(() -> {
            RDFParser.source("kway.ttl").parse(stream);
        });
        
        //
        executor.shutdown();
        
        //
        executor = Executors.newFixedThreadPool(4 * Runtime.getRuntime().availableProcessors());
                 
        //
        Tagger tagger = new Tagger();
        
        while (iterator.hasNext()) {
            //
            final Node object = iterator.next().getObject();
                
            //
            if (!object.isLiteral()) {
                continue;
            }
            
            executor.execute(() -> {                             
                //               
                for (Sentence sentence : tagger.tag(object.getLiteralValue().toString())) {
                    //
                    for(Word word : sentence) {                                                 
                        //
                        for(Morpheme morpheme : word){
                            //
                            System.out.println(word.getSurface() + " " + morpheme.getSurface() + " " + morpheme.getTag());
                            //break;
                        }                    
                    } 
                    //break;
                }   
                //break;
            });              
        }
         
        //        
        executor.shutdown();
    }        
         
}
