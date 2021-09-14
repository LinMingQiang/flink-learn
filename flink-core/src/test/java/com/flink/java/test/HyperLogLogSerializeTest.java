package com.flink.java.test;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.RegisterSet;
import com.google.common.hash.BloomFilter;

import java.io.*;
import java.util.HashMap;

public class HyperLogLogSerializeTest {
    /**
     * HyperLogLog序列化测试
     * @param args
     */
    public static void main(String[] args) throws Exception {
        // 0.0075 和0.00075，对象大小差100倍，大小只跟精度有关系
//        HyperLogLog hll = new HyperLogLog(0.0055);
//        hll.offer("1");
//        hll.offer("2");
//        hll.offer("3");
//        hll.offer("4");
//        hll.offer("1");
//        for (int i = 0; i < 10000; i++) {
//            hll.offer(""+i);
//        }
        HashMap<String, String> h = new HashMap<>();  // 10w - 4290k
        HyperLogLog hll = new HyperLogLog(0.0075); // 11k
        // hll的大小只和精度有关
        for(int i = 0; i< 10000; i++){
            hll.offer(""+i);
        }
        System.out.println(hll.cardinality());
        System.out.println(hll.offer("11111") + ":" + hll.cardinality()); // true
        System.out.println(hll.offer("1") + ":" + hll.cardinality()); // true
        System.out.println(hll.offer("2") + ":" + hll.cardinality()); // false
        System.out.println(hll.offer("r") + ":" + hll.cardinality()); // false
        System.out.println(hll.offer("44444444") + ":" + hll.cardinality()); // true
        System.out.println(hll.offer("11111111") + ":" + hll.cardinality()); // false
        System.out.println(hll.offer("aa") + ":" + hll.cardinality()); // false
//        System.out.println(hll.offerHashed("11111".hashCode()) + ":" + hll.cardinality()); // true
//        System.out.println(hll.offerHashed("1".hashCode()) + ":" + hll.cardinality()); // true
//        System.out.println(hll.offerHashed("2".hashCode()) + ":" + hll.cardinality()); // false
//        System.out.println(hll.offerHashed("r".hashCode()) + ":" + hll.cardinality()); // false
//        System.out.println(hll.offerHashed("44444444".hashCode()) + ":" + hll.cardinality()); // true
//        System.out.println(hll.offerHashed("11111111".hashCode()) + ":" + hll.cardinality()); // false
//        System.out.println(hll.offerHashed("aa".hashCode()) + ":" + hll.cardinality()); // false
//        SerializableObjToFile(hll);
//        SerializableObjToFile(h);
//        byte[] bys = objectToByteArray(hll);
//        HyperLogLog hll2 = (HyperLogLog) byteArrayToObject(bys);
//
//        SerializableObjToFile(hll);
//        HyperLogLog hll2 = (HyperLogLog) DeserializableBloomFilter();
//
//        System.out.println(hll2.cardinality());
//        hll2.offer("5");
//        hll2.offer("5");
//        System.out.println(hll2.cardinality());
    }


    // 序列化成文件
    public static  void SerializableObjToFile(Object obj) throws IOException {
        FileOutputStream fileOut = new FileOutputStream("/Users/eminem/workspace/flink/flink-learn/resources/file/BloomFilter.ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(obj);
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
    // 序列化成字节数组
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
