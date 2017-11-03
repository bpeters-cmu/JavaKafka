/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.upskafka;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import kafka.admin.RackAwareMode;


public class KafkaGo {    

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger("main");
        KProducer p = new KProducer();
        p.produce();
        KConsumer.consume();
        
        
    }

    
    
}