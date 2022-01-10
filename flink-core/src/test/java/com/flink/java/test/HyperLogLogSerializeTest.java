package com.flink.java.test;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

import java.io.*;

public class HyperLogLogSerializeTest {
    /**
     * HyperLogLog序列化测试
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        // 0.0075 和0.00075，对象大小差100倍，大小只跟精度有关系
        // 0.0075：11kb 0.005：22k
        // 1%:6k 3%:800b
        //        HashMap<String, String> h = new HashMap<>();  // 10w - 4290k
        HyperLogLog hll = new HyperLogLog(0.03); // 11k
        for (int i = 0; i < 1000000; i++) {
            hll.offer(i + ":");
        }
        System.out.println(hll.cardinality());
        SerializableBytesToFile(hll);
        // hll的大小只和精度有关
        //        SerializableBytesToFile(hll.serialize().getBytes());
        //        SerializableObjToFile(hll.serialize().getBytes());
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
    public static void SerializableObjToFile(byte[] bytes) throws IOException {
        FileOutputStream fileOut =
                new FileOutputStream(
                        "/Users/eminem/workspace/flink/flink-learn/resources/file/hyperloglog.ser");
        fileOut.write(bytes, 0, bytes.length);
        fileOut.flush();
        fileOut.close();
        System.out.println("Serialized data is saved in /tmp/employee.ser");
    }

    // 序列化成文件
    public static void SerializableBytesToFile(Object obj) throws IOException {
        FileOutputStream fileOut =
                new FileOutputStream(
                        "/Users/eminem/workspace/flink/flink-learn/resources/file/hyperloglog.ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(obj);
        out.close();
        fileOut.close();
        System.out.println("Serialized data is saved in /tmp/employee.ser");
    }

    public static Object DeserializableBloomFilter() throws IOException, ClassNotFoundException {
        FileInputStream fileIn =
                new FileInputStream(
                        "/Users/eminem/workspace/flink/flink-learn/resources/file/hyperloglog.ser");
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
