package com.flink.learn.sql.func;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.clearspring.analytics.util.Bits;
import com.clearspring.analytics.util.IBuilder;

import java.io.*;

public class HyperLogLog implements ICardinality, Serializable {

    public  RegisterSet registerSet;
    public  int log2m;
    public  double alphaMM;

    public HyperLogLog() {
        this(log2m(0.05));
    }

    /**
     * Create a new HyperLogLog instance using the specified standard deviation.
     *
     * @param rsd - the relative standard deviation for the counter.
     *            smaller values create counters that require more space.
     */
    public HyperLogLog(double rsd) {
        this(log2m(rsd));
    }

    private static int log2m(double rsd) {
        return (int) (Math.log((1.106 / rsd) * (1.106 / rsd)) / Math.log(2));
    }

    /**
     * Create a new HyperLogLog instance.  The log2m parameter defines the accuracy of
     * the counter.  The larger the log2m the better the accuracy.
     * <p/>
     * accuracy = 1.04/sqrt(2^log2m)
     *
     * @param log2m - the number of bits to use as the basis for the HLL instance
     */
    public HyperLogLog(int log2m) {
        this(log2m, new RegisterSet(1 << log2m));
    }

    /**
     * Creates a new HyperLogLog instance using the given registers.  Used for unmarshalling a serialized
     * instance and for merging multiple counters together.
     *
     * @param registerSet - the initial values for the register set
     */
    public HyperLogLog(int log2m, RegisterSet registerSet) {
        if (log2m < 0 || log2m > 30) {
            throw new IllegalArgumentException("log2m argument is "
                    + log2m + " and is outside the range [0, 30]");
        }
        this.registerSet = registerSet;
        this.log2m = log2m;
        int m = 1 << this.log2m;

        alphaMM = getAlphaMM(log2m, m);
    }

    @Override
    public boolean offerHashed(long hashedValue) {
        // j becomes the binary address determined by the first b log2m of x
        // j will be between 0 and 2^log2m
        final int j = (int) (hashedValue >>> (Long.SIZE - log2m));
        final int r = Long.numberOfLeadingZeros((hashedValue << this.log2m) | (1 << (this.log2m - 1)) + 1) + 1;
        return registerSet.updateIfGreater(j, r);
    }

    @Override
    public boolean offerHashed(int hashedValue) {
        // j becomes the binary address determined by the first b log2m of x
        // j will be between 0 and 2^log2m
        final int j = hashedValue >>> (Integer.SIZE - log2m);
        final int r = Integer.numberOfLeadingZeros((hashedValue << this.log2m) | (1 << (this.log2m - 1)) + 1) + 1;
        return registerSet.updateIfGreater(j, r);
    }

    @Override
    public boolean offer(Object o) {
        final int x = MurmurHash.hash(o);
        return offerHashed(x);
    }


    @Override
    public long cardinality() {
        double registerSum = 0;
        int count = registerSet.count;
        double zeros = 0.0;
        for (int j = 0; j < registerSet.count; j++) {
            int val = registerSet.get(j);
            registerSum += 1.0 / (1 << val);
            if (val == 0) {
                zeros++;
            }
        }

        double estimate = alphaMM * (1 / registerSum);

        if (estimate <= (5.0 / 2.0) * count) {
            // Small Range Estimate
            return Math.round(linearCounting(count, zeros));
        } else {
            return Math.round(estimate);
        }
    }

    @Override
    public int sizeof() {
        return registerSet.size * 4;
    }

    @Override
    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new DataOutputStream(baos);
        writeBytes(dos);

        return baos.toByteArray();
    }

    private void writeBytes(DataOutput serializedByteStream) throws IOException {
        serializedByteStream.writeInt(log2m);
        serializedByteStream.writeInt(registerSet.size * 4);
        for (int x : registerSet.readOnlyBits()) {
            serializedByteStream.writeInt(x);
        }
    }

    /**
     * Add all the elements of the other set to this set.
     * <p/>
     * This operation does not imply a loss of precision.
     *
     * @param other A compatible Hyperloglog instance (same log2m)
     * @throws CardinalityMergeException if other is not compatible
     */
    public void addAll(HyperLogLog other) throws CardinalityMergeException {
        if (this.sizeof() != other.sizeof()) {
            throw new HyperLogLogMergeException("Cannot merge estimators of different sizes");
        }

        registerSet.merge(other.registerSet);
    }

    @Override
    public ICardinality merge(ICardinality... estimators) throws CardinalityMergeException {
        HyperLogLog merged = new HyperLogLog(log2m, new RegisterSet(this.registerSet.count));
        merged.addAll(this);

        if (estimators == null) {
            return merged;
        }

        for (ICardinality estimator : estimators) {
            if (!(estimator instanceof HyperLogLog)) {
            }
            HyperLogLog hll = (HyperLogLog) estimator;
            merged.addAll(hll);
        }

        return merged;
    }

    private Object writeReplace() {
        return new SerializationHolder(this);
    }


    /**
     * This class exists to support Externalizable semantics for
     * HyperLogLog objects without having to expose a public
     * constructor, public write/read methods, or pretend final
     * fields aren't final.
     *
     * In short, Externalizable allows you to skip some of the more
     * verbose meta-data default Serializable gets you, but still
     * includes the class name. In that sense, there is some cost
     * to this holder object because it has a longer class name. I
     * imagine people who care about optimizing for that have their
     * own work-around for long class names in general, or just use
     * a custom serialization framework. Therefore we make no attempt
     * to optimize that here (eg. by raising this from an inner class
     * and giving it an unhelpful name).
     */
    private static class SerializationHolder implements Externalizable {

        HyperLogLog hyperLogLogHolder;

        public SerializationHolder(HyperLogLog hyperLogLogHolder) {
            this.hyperLogLogHolder = hyperLogLogHolder;
        }

        /**
         * required for Externalizable
         */
        public SerializationHolder() {

        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            hyperLogLogHolder.writeBytes(out);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            hyperLogLogHolder = Builder.build(in);
        }

        private Object readResolve() {
            return hyperLogLogHolder;
        }
    }

    public static class Builder implements IBuilder<ICardinality>, Serializable {

        private double rsd;

        public Builder(double rsd) {
            this.rsd = rsd;
        }

        @Override
        public HyperLogLog build() {
            return new HyperLogLog(rsd);
        }

        @Override
        public int sizeof() {
            int log2m = log2m(rsd);
            int k = 1 << log2m;
            return RegisterSet.getBits(k) * 4;
        }

        public static HyperLogLog build(byte[] bytes) throws IOException {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            return build(new DataInputStream(bais));
        }

        public static HyperLogLog build(DataInput serializedByteStream) throws IOException {
            int log2m = serializedByteStream.readInt();
            int byteArraySize = serializedByteStream.readInt();
            return new HyperLogLog(log2m,
                    new RegisterSet(1 << log2m, Bits.getBits(serializedByteStream, byteArraySize)));
        }
    }

    @SuppressWarnings("serial")
    protected static class HyperLogLogMergeException extends CardinalityMergeException {

        public HyperLogLogMergeException(String message) {
            super(message);
        }
    }

    protected static double getAlphaMM(final int p, final int m) {
        // See the paper.
        switch (p) {
            case 4:
                return 0.673 * m * m;
            case 5:
                return 0.697 * m * m;
            case 6:
                return 0.709 * m * m;
            default:
                return (0.7213 / (1 + 1.079 / m)) * m * m;
        }
    }
    public static int log2m(Double rsd) {
        return (int)(Math.log((1.106 / rsd) * (1.106 / rsd)) / Math.log(2));
    }

    protected static double linearCounting(int m, double V) {
        return m * Math.log(m / V);
    }
}
