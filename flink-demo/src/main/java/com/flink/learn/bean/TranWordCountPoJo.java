package com.flink.learn.bean;


import java.util.Arrays;
import java.util.Objects;

public class TranWordCountPoJo {
    public String word = "";
    public Long count = 2L;
    public Long timestamp = 2L;
	public String[] srcArr = null;
	public WordCountGroupByKey keyby = null;

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof TranWordCountPoJo)) return false;
		TranWordCountPoJo that = (TranWordCountPoJo) o;
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

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public String[] getSrcArr() {
		return srcArr;
	}

	public void setSrcArr(String[] srcArr) {
		this.srcArr = srcArr;
	}

	public WordCountGroupByKey getKeyby() {
		return keyby;
	}

	public void setKeyby(WordCountGroupByKey keyby) {
		this.keyby = keyby;
	}

	public TranWordCountPoJo(String word, Long count, Long timestamp, String[] srcArr, WordCountGroupByKey keyby) {
		this.word = word;
		this.count = count;
		this.timestamp = timestamp;
		this.srcArr = srcArr;
		this.keyby = keyby;
	}
}
