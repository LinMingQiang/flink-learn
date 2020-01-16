package com.flink.learn.bean;


public class WordCountPoJo {
    public String w = "ss";
    public Long c = 2L;
    public Long timestamp = 2L;

	@Override
	public String toString() {
		return "WordCountPoJo{" +
			"w='" + w + '\'' +
			", c=" + c +
			", timestamp=" + timestamp +
			'}';
	}
}
