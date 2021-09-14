package com.flink.java.test;

import com.flink.common.dbutil.FileSystemHandler;
import com.flink.common.dbutil.MongoDBFactory;
import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.pojo.BloomFilterScalable;
import org.bson.Document;
import org.bson.types.Binary;

import java.io.*;
import java.util.ArrayList;

public class GuavaBloomFilterTest {
    // 布隆过滤器序列化测试
    public static void main(String[] args) throws Exception {
        //
        // 1亿 = 120m， fpp为误差， 0.01 120m， 0.001 180m，序列化时间也比较久
//        BloomFilter<CharSequence> bloom = BloomFilter.create(
//                Funnels.stringFunnel(),
//                100000000, 0.01);
//        for (int i = 0; i <= 100000; i++) {
//            bloom.put("" + i);
//        }
//        SerializableBloomFilter(bloom);
//        BloomFilter<CharSequence> bloomFilter2 = DeserializableBloomFilter();

//        byte[] b = objectToByteArray(bloomFilter);
//        BloomFilter<CharSequence> bloomFilter2 = (BloomFilter<CharSequence>) byteArrayToObject(b);

        BloomFilterScalable bloom = new BloomFilterScalable();
        for (int i = 0; i <= 100000; i++) {
            bloom.put("" + i);
        }
        System.out.println(bloom.currentUV);
        storageToFileSystem(bloom);
        System.out.println(bloom.mightContain("1"));
        System.out.println(bloom.mightContain("999s1"));
    }

    public static void SerializableBloomFilter(BloomFilter<CharSequence> bloomFilter) throws IOException {
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
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(obj);
        objectOutputStream.flush();
        return byteArrayOutputStream.toByteArray();
    }

    public static Object byteArrayToObject(byte[] bytes) throws Exception {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        return objectInputStream.readObject();
    }

    /**
     * mongo不支持bytes
     * @param obj
     * @throws IOException
     */
    public static void storageToFileSystem(Object obj) throws Exception {
        String filePath = "file:///Users/eminem/workspace/flink/flink-learn/checkpoint/obj";
        FileSystemHandler fs = new FileSystemHandler();
        fs.writeObject(obj, filePath);
        BloomFilterScalable bloomFilter = (BloomFilterScalable) fs.readObject(filePath);
        System.out.println(bloomFilter.mightContain("1"));
        System.out.println(bloomFilter.mightContain("999s1"));
    }
}
