package com.pojo;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * 布隆过滤器，可以动态扩容
 */
public class BloomFilterScalable implements Serializable {
    // 记录没个布隆的存储个数
    public ArrayList<BloomFilterInfo> bloomFilterList = new ArrayList<>();
    public Long lastTimeExpansion = System.currentTimeMillis(); // 上次扩容时间，用来计算下次扩容大小，如果增速快就加大
    public int expectedInsertions = 100000; // 差不多 0.1M
    public Double fpp = 0.01;
    public Long currentUV = 0L;
    public Double nextRatio = 0.5; // 前面扩展得快，后面慢，最终到0.5

    public ArrayList<BloomFilterInfo> getBloomFilterList() {
        return bloomFilterList;
    }

    public void setBloomFilterList(ArrayList<BloomFilterInfo> bloomFilterList) {
        this.bloomFilterList = bloomFilterList;
    }

    public int getExpectedInsertions() {
        return expectedInsertions;
    }

    public void setExpectedInsertions(int expectedInsertions) {
        this.expectedInsertions = expectedInsertions;
    }

    public Double getFpp() {
        return fpp;
    }

    public BloomFilterScalable() {
    }

    public void setFpp(Double fpp) {
        this.fpp = fpp;
    }

    public Long getCurrentUV() {
        return currentUV;
    }


    /**
     * 有误差率，比实际要少1% - 3% ，这里用这个方式的会乘上
     *
     * @return
     */
    public Long getRealCurrentUV(int ratio) {
        return (currentUV * (100+ratio))/100L;
    }

    public void setCurrentUV(Long currentUV) {
        this.currentUV = currentUV;
    }

    public BloomFilterScalable(int expectedInsertions, Double fpp) {
        this.expectedInsertions = expectedInsertions;
        this.fpp = fpp;
        bloomFilterList.add(new BloomFilterInfo(expectedInsertions, fpp));
    }

    /**
     * 初始化一个布隆过滤器，但是这里是动态扩容的
     *
     * @param id
     * @return
     */
    public BloomFilterInfo instance(String id) {
        if (currentUV > 0) { // 非第一次扩容
            Long expansionIntervalMin = (System.currentTimeMillis() - lastTimeExpansion) / 60000;
            if (expansionIntervalMin < 10) { // 10分钟就要再扩容的，2倍
                nextRatio = 1.0;
            } else if (expansionIntervalMin >= 10 && expansionIntervalMin < 60) {
                nextRatio = 0.7;
            } else nextRatio = 0.5;
            expectedInsertions = (int) (currentUV * nextRatio); // 翻倍往上初始化，10w -》 20 -》 40w
            lastTimeExpansion = System.currentTimeMillis();
        }
        BloomFilterInfo bloomfilterinfo = new BloomFilterInfo(expectedInsertions, fpp);
        bloomfilterinfo.put(id);
        return bloomfilterinfo;
    }

    /**
     * 插入的同时返回是否为新id
     * @param id
     * @return true表示新id
     */
    public boolean put(String id) {
        boolean hasPut = false;
        boolean newId = false;
        for (int i = 0; i < bloomFilterList.size(); i++) {
            BloomFilterInfo element = bloomFilterList.get(i);
            if (!element.isFull()) { // 当前布隆没满
                if (element.put(id)) { // 表示插入了一个新值
                    currentUV += 1;
                    newId = true;
                }
                hasPut = true;
                break;
            } else { // 满了，要判断一下存不存在
                if(element.mightContain(id)) {
                    hasPut = true;
                    break;
                }
            }
        }
        if (!hasPut) { // 新建一个布隆
            currentUV += 1;
            BloomFilterInfo n = instance(id);
            bloomFilterList.add(n);
        }
        return newId;
    }


    /**
     * @param id
     * @return
     */
    public boolean mightContain(String id) {
        for (int i = 0; i < bloomFilterList.size(); i++) {
            if (bloomFilterList.get(i).mightContain(id)) { // 存在就返回
                return true;
            }
        }
        return false;
    }


    public int getBloomNum() {
        return bloomFilterList.size();
    }


    public class BloomFilterInfo implements Serializable {
        public BloomFilter<CharSequence> bloomFilter;
        public Long bloomUv = 0L;
        public int expectedInsertions;

        public boolean isFull() {
            return bloomUv >= (int) (expectedInsertions * 0.9);
        }

        /**
         * bloomFilter.put(id) ： 当布隆发生了变化返回true表示一个新值，如果返回false也有可能是一个新值，
         * 如果用put的返回值来判断是否新值，会导致结果偏小，
         * 用mightContain 来累积会导致结果偏小
         *
         * @param id
         * @return
         */
        public boolean put(String id) {
            boolean isNew = bloomFilter.put(id);
            if (isNew) { // 表示插入了一个新值
                bloomUv += 1;
            }
            return isNew;
        }

        public boolean mightContain(String id) {
            return bloomFilter.mightContain(id);
        }

        public BloomFilterInfo(int expectedInsertions, Double fpp) {
            this.bloomFilter = BloomFilter.create(
                    Funnels.stringFunnel(),
                    expectedInsertions, fpp);
            ;
            this.expectedInsertions = expectedInsertions;
        }

        public BloomFilterInfo() {
        }

        public BloomFilter<CharSequence> getBloomFilter() {
            return bloomFilter;
        }

        public void setBloomFilter(BloomFilter<CharSequence> bloomFilter) {
            this.bloomFilter = bloomFilter;
        }

        public Long getBloomUv() {
            return bloomUv;
        }

        public void setBloomUv(Long bloomUv) {
            this.bloomUv = bloomUv;
        }

        public int getExpectedInsertions() {
            return expectedInsertions;
        }

        public void setExpectedInsertions(int expectedInsertions) {
            this.expectedInsertions = expectedInsertions;
        }
    }
}
