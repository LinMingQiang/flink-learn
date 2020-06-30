package com.flink.learn.bean;


public class WordCountPoJo {
    public String word = "ss";
    public Long count = 2L;
    public Long timestamp = 2L;

	@Override
	public String toString() {
		return "WordCountPoJo{" +
			"w='" + word + '\'' +
			", c=" + count +
			", timestamp=" + timestamp +
			'}';
	}
}
