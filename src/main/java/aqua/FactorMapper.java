package aqua;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FactorMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Snapshot prevSnapshot = null;
    private Text outKey = new Text();
    private Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // 过滤非数据行
        if (line.startsWith("tradingDay")) return;

        try {
            Snapshot currSnapshot = new Snapshot(line);

            // --- 核心逻辑调整 ---

            // 1. 判断是否是 9:30 之前的数据 (例如 092957)
            if (currSnapshot.tradeTime < 92957) {
                // 如果是开盘前的数据，只更新 prevSnapshot，用于辅助下一次计算
                // 绝对不要 context.write
                return; // 直接结束，不输出
            }

            if (currSnapshot.tradeTime == 92957) {
                // 如果是 9:29:57 的数据，直接缓存，不计算
                prevSnapshot = currSnapshot;
                return; // 直接结束，不输出
            }
            // 2. 如果时间 >= 9:30，执行计算
            // 此时 prevSnapshot 可能就是 092957 那一行的数据
            String resultFactors = FactorCalculator.calculateAll(currSnapshot, prevSnapshot);

            // 3. 输出 (PDF要求: 输出时刻与每个因子的平均值)
            String timeStr = String.format("%06d", currSnapshot.tradeTime);
            outKey.set(currSnapshot.tradingDay + "," + timeStr);
            outVal.set(resultFactors);
            context.write(outKey, outVal);

            // 4. 更新缓存
            prevSnapshot = currSnapshot;

        } catch (Exception e) {
            // 忽略解析错误
        }
    }
}