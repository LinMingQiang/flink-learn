package com.flink.common.dbutil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

public class FileSystemHandler {
    FileSystem fs = null;

    public FileSystemHandler() throws IOException {
        fs = FileSystem.get(new Configuration());
    }

    public void writeObject(Object obj, String filePath) throws IOException {
        FSDataOutputStream output = fs.create(new Path(filePath));
        byte[] bytes = FileSystemHandler.objectToByteArray(obj);
        output.write(bytes);
        output.close();
    }

    public Object readObject(String filePath) throws Exception {
        FSDataInputStream in = fs.open(new Path(filePath));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copyBytes(in, out, 4096, false);
        in.close();
        Object obj = byteArrayToObject(out.toByteArray());
        out.close();
        return obj;
    }

    public static byte[] objectToByteArray(Object obj) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(obj);
        objectOutputStream.flush();
        byte[] bytes = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.close();
        objectOutputStream.close();
        return bytes;
    }

    public static Object byteArrayToObject(byte[] bytes) throws Exception {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        Object obj = objectInputStream.readObject();
        byteArrayInputStream.close();
        objectInputStream.close();
        return obj;
    }
}
