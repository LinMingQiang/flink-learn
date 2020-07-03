package com.flink.learn.bean;


import java.io.Serializable;
import java.util.Objects;

public class WordCountPoJo implements Serializable {
    public String word = "ss";
    public Long count = 2L;
    public Long timestamp = 2L;
	@Override
	public boolean equals(Object obj) {
		return super.equals(obj);
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

	@Override
	public int hashCode() {
		return Objects.hash(word, count, timestamp);
	}

	public WordCountPoJo(){

	}

	public WordCountPoJo(String word, Long count, Long timestamp){
setWord(word);
setCount(count);
setTimestamp(timestamp);
	}


	@Override
	public String toString() {
		return "WordCountPoJo{" +
			"w='" + word + '\'' +
			", c=" + count +
			", timestamp=" + timestamp +
			'}';
	}
}
