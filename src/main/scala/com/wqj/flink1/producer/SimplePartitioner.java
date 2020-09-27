package com.wqj.flink1.producer;


import org.apache.kafka.clients.producer.Partitioner;

//public class SimplePartitioner implements Partitioner {
//    public SimplePartitioner (VerifiableProperties props) {
//
//    }
//
//    public int partition(Object key, int a_numPartitions) {
//        int partition = 0;
//        String stringKey = (String) key;
//        int offset = stringKey.lastIndexOf('.');
//        if (offset > 0) {
//            partition = Integer.parseInt( stringKey.substring(offset+1)) % a_numPartitions;
//        }
//        return partition;
//    }
//
//}
