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

	@Override
	public int hashCode() {
		return Objects.hash(word, count, timestamp);
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
