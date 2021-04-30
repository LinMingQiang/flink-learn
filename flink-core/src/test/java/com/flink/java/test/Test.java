package com.flink.java.test;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

import java.io.*;
import java.util.HashMap;
import java.util.UUID;

public class Test {

    public static void main(String[] args) throws IOException {
        HashMap<String, Boolean> map= new HashMap<>();
        for (int i = 0; i < 1000000; i++) {
            map.put(UUID.randomUUID().toString(), true);
        }

        SerializableBloomFilter(map);
//
//        Long start = System.currentTimeMillis();
//        for (int i = 0; i < 10000; i++) {
//            map.get("sss" + i);
//        }
//        Long end = System.currentTimeMillis();
//
//        System.out.printf("" + (end - start));
    }


    public static  void SerializableBloomFilter(HashMap<String, Boolean> bloomFilter) throws IOException {
        FileOutputStream fileOut = new FileOutputStream("/Users/eminem/workspace/flink/flink-learn/resources/file/BloomFilter.ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(bloomFilter);
        out.close();
        fileOut.close();
        System.out.println("Serialized data is saved in /tmp/employee.ser");
    }


    public static Object DeserializableBloomFilter() throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream("/Users/eminem/workspace/flink/flink-learn/resources/file/BloomFilter.ser");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Object e = in.readObject();
        in.close();
        fileIn.close();
        System.out.println("Serialized data is saved in /tmp/employee.ser");
        return e;
    }
}
