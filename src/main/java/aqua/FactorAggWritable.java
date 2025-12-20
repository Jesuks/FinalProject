package aqua;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FactorAggWritable implements Writable {
    public static final int N = 20;

    private final double[] sum = new double[N];
    private long count;

    public FactorAggWritable() {}

    public double[] sum() { return sum; }
    public long count() { return count; }

    public void clear() {
        for (int i = 0; i < N; i++) sum[i] = 0.0;
        count = 0L;
    }

    public void setFromCsv20(String csv) {
        // csv: "f1,f2,...,f20"
        clear();
        count = 1L;

        int idx = 0;
        int start = 0;
        int len = csv.length();
        for (int i = 0; i <= len && idx < N; i++) {
            if (i == len || csv.charAt(i) == ',') {
                // 这里 substring 会分配，但只做 20 次；相比 reducer 144万次 split/parse 代价小太多
                if (i > start) {
                    sum[idx] = Double.parseDouble(csv.substring(start, i));
                } else {
                    sum[idx] = 0.0;
                }
                idx++;
                start = i + 1;
            }
        }
        // 不足 20 个则剩余保持 0
    }

    public void add(FactorAggWritable other) {
        double[] o = other.sum;
        for (int i = 0; i < N; i++) sum[i] += o[i];
        count += other.count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(count);
        for (int i = 0; i < N; i++) out.writeDouble(sum[i]);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readLong();
        for (int i = 0; i < N; i++) sum[i] = in.readDouble();
    }
}