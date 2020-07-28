package com.flink.common.java.pojo;

public class WordCountPoJo {
    public String word = "";
    public long num = 0L;

    public WordCountPoJo(String word, long num) {
        this.word = word;
        this.num = num;
    }

    @Override
    public String toString() {
        return "WordCountPoJo{" +
                "word='" + word + '\'' +
                ", num=" + num +
                '}';
    }

    public WordCountPoJo() {
    }
}
