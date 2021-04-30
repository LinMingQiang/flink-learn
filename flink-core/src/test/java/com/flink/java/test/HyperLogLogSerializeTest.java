package com.flink.java.test;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.RegisterSet;
import com.google.common.hash.BloomFilter;

import java.io.*;

public class HyperLogLogSerializeTest {
    /**
     * HyperLogLog序列化测试
     * @param args
     */
    public static void main(String[] args) throws Exception {
        // 0.0075 和0.00075，对象大小差100倍，大小只跟精度有关系
        HyperLogLog hll = new HyperLogLog(0.0055);
        hll.offer("1");
        hll.offer("2");
        hll.offer("3");
        hll.offer("4");
        hll.offer("1");
        for (int i = 0; i < 10000; i++) {
            hll.offer(""+i);
        }
//        byte[] bys = objectToByteArray(hll);
//        HyperLogLog hll2 = (HyperLogLog) byteArrayToObject(bys);

        SerializableBloomFilter(hll);
        HyperLogLog hll2 = (HyperLogLog) DeserializableBloomFilter();

        System.out.println(hll2.cardinality());
        hll2.offer("5");
        hll2.offer("5");
        System.out.println(hll2.cardinality());
    }


    public static  void SerializableBloomFilter(HyperLogLog bloomFilter) throws IOException {
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
