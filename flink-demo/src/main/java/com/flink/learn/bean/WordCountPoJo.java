package com.flink.learn.bean;


import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class WordCountPoJo implements Serializable {
    public String word = "ss";
    public Long count = 2L;
    public Long timestamp = 2L;
    public String[] srcArr = null;
    public WordCountGroupByKey keyby = null;

    public WordCountGroupByKey getKeyby() {
        return keyby;
    }

    public void setKeyby(WordCountGroupByKey keyby) {
        this.keyby = keyby;
    }

    public String getWord() {
        return word;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setWord(String word) {
        this.word = word;
    }


    public WordCountPoJo() {

    }

    public Long getCount() {
        return count;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String[] getSrcArr() {
        return srcArr;
    }

    public void setSrcArr(String[] srcArr) {
        this.srcArr = srcArr;
    }


    public WordCountPoJo(String word, Long count, Long timestamp, String[] srcArr, WordCountGroupByKey keyby) {
        this.word = word;
        this.count = count;
        this.timestamp = timestamp;
        this.srcArr = srcArr;
        this.keyby = keyby;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WordCountPoJo that = (WordCountPoJo) o;
        return Objects.equals(word, that.word) &&
                Objects.equals(count, that.count) &&
                Objects.equals(timestamp, that.timestamp) &&
                Arrays.equals(srcArr, that.srcArr) &&
                Objects.equals(keyby, that.keyby);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(word, count, timestamp, keyby);
        result = 31 * result + Arrays.hashCode(srcArr);
        return result;
    }

    @Override
    public String toString() {
        return "WordCountPoJo{" +
                "word='" + word + '\'' +
                ", count=" + count +
                ", timestamp=" + timestamp +
                ", srcArr=" + Arrays.toString(srcArr) +
                ", keyby=" + keyby +
                '}';
    }
}
