package com.pojo;

import java.util.Objects;

public class WordCountPoJo {
    public String word;
    public long num;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WordCountPoJo)) return false;
        WordCountPoJo that = (WordCountPoJo) o;
        return num == that.num &&
                Objects.equals(word, that.word);
    }

    @Override
    public int hashCode() {
        return Objects.hash(word, num);
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getNum() {
        return num;
    }

    public void setNum(long num) {
        this.num = num;
    }

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
