package com.flink.java.test;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.*;

public class GuavaBloomFilterTest {
    public static void main(String[] args) throws Exception {
        BloomFilter<CharSequence> bloomFilter = BloomFilter.create(
                Funnels.stringFunnel(Charsets.UTF_8),
                100000000,0.01);
        for(int i =0; i<=1000000; i++){
            bloomFilter.put("" + i);
        }
//        SerializableBloomFilter(bloomFilter);
//        BloomFilter<CharSequence> bloomFilter2 = DeserializableBloomFilter();

//        byte[] b = objectToByteArray(bloomFilter);
//        BloomFilter<CharSequence> bloomFilter2 = (BloomFilter<CharSequence>) byteArrayToObject(b);

        System.out.println(bloomFilter.mightContain("1"));
        System.out.println(bloomFilter.mightContain("22444"));
    }

    public static  void SerializableBloomFilter(BloomFilter<CharSequence> bloomFilter) throws IOException {
        FileOutputStream fileOut = new FileOutputStream("/Users/eminem/workspace/flink/flink-learn/resources/file/BloomFilter.ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(bloomFilter);
        out.close();
        fileOut.close();
        System.out.printf("Serialized data is saved in /tmp/employee.ser");
    }


    public static Object DeserializableBloomFilter() throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream("/Users/eminem/workspace/flink/flink-learn/resources/file/BloomFilter.ser");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Object e = in.readObject();
        in.close();
        fileIn.close();
        System.out.printf("Serialized data is saved in /tmp/employee.ser");
        return e;
    }


    public static byte[] objectToByteArray(Object obj) throws IOException {
        byte[] bytes = null;
        ByteArrayOutputStream byteArrayOutputStream = null;
        ObjectOutputStream objectOutputStream = null;
            byteArrayOutputStream = new ByteArrayOutputStream();
            objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(obj);
            objectOutputStream.flush();
            return byteArrayOutputStream.toByteArray();
    }

    public static Object byteArrayToObject(byte[] bytes) throws Exception {
        Object obj = null;
        ByteArrayInputStream byteArrayInputStream = null;
        ObjectInputStream objectInputStream = null;
            byteArrayInputStream = new ByteArrayInputStream(bytes);
            objectInputStream = new ObjectInputStream(byteArrayInputStream);
            obj = objectInputStream.readObject();
        return obj;
    }
}
