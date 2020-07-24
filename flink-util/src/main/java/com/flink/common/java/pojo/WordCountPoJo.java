package com.flink.common.java.pojo;

public class WordCountPoJo {
    public String word = "";
    public long count = 0L;

    public WordCountPoJo(String word, long count) {
        this.word = word;
        this.count = count;
    }

    public WordCountPoJo() {
    }
}
