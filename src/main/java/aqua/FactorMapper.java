package aqua;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FactorMapper extends Mapper<LongWritable, Text, Text, FactorAggWritable> {

    private Snapshot prevSnapshot = null;
    private Text outKey = new Text();
    private FactorAggWritable outVal = new FactorAggWritable();

    private final StringBuilder keyBuilder = new StringBuilder(32);

    // 09:29:57
    private static final int CUTOFF = 92957;
    private final double[] factorsBuf = new double[20];
    private long lastOffset = -1L;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        long off = key.get();
        if (off == 0L && lastOffset > 0L) {
            // 新文件开始
            prevSnapshot = null;
        }
        lastOffset = off;

        String line = value.toString();
        // 过滤非数据行
        if (line.startsWith("tradingDay")) return;
        int c1 = line.indexOf(',');
        if (c1 < 0) return;
        int c2 = line.indexOf(',', c1 + 1);
        if (c2 < 0) return;

        final int tradeTime;
        try {
            tradeTime = Integer.parseInt(line.substring(c1 + 1, c2).trim());
        } catch (Exception ignore) {
            return;
        }

// < 09:29:57 直接丢弃（不构造 Snapshot）
        if (tradeTime < 92957) return;
        try {
            Snapshot currSnapshot = new Snapshot(line);

            if (tradeTime == 92957) {
                // 09:29:57 时刻，初始化 prevSnapshot 并跳过输出
                prevSnapshot = currSnapshot;
                return;
            }
            FactorCalculator.calculateAll(currSnapshot, prevSnapshot, factorsBuf);
            // ===== 输出 key：tradingDay,HHMMSS=====
            keyBuilder.setLength(0);
            // tradingDay 就是第一列：直接从原行 append(0,c1)，避免 substring 新建 String
            keyBuilder.append(line, 0, c1).append(',');
            appendPadded6(keyBuilder, tradeTime);

            outKey.set(keyBuilder.toString());
            outVal.setFromDoubles(factorsBuf);
            context.write(outKey, outVal);

            // 更新缓存
            prevSnapshot = currSnapshot;

        } catch (Exception e) {
            // 忽略解析错误
        }
    }
    private static void appendPadded6(StringBuilder sb, int v) {
        if (v < 0) { sb.append('-'); v = -v; }
        if (v < 100000) sb.append('0');
        if (v < 10000) sb.append('0');
        if (v < 1000) sb.append('0');
        if (v < 100) sb.append('0');
        if (v < 10) sb.append('0');
        sb.append(v);
    }
}