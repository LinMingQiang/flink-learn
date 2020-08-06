package com.flink.learn.bean;

import java.util.Objects;

/**
 * hashCode 和 equals是必须的，否则无法做key
 */
public class WordCountGroupByKey {
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String key = "";

    public WordCountGroupByKey(){

    }
    public WordCountGroupByKey(String key){
      this.key = key;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WordCountGroupByKey that = (WordCountGroupByKey) o;
        return Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    @Override
    public String toString() {
        return "WordCountGroupByKey{" +
                "key='" + key + '\'' +
                '}';
    }
}
