package aqua;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 将同一天(tradingDay)的 key 全部分到同一个 reducer。
 * key 格式： "YYYYMMDD,HHMMSS"
 */
public class TradingDayPartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        String s = key.toString();
        int comma = s.indexOf(',');
        String day = (comma >= 0) ? s.substring(0, comma) : s;
        return (day.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}