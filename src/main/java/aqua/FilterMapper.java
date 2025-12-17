package aqua;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FilterMapper {

    public static class ETLMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outLine = new Text();
        private StringBuilder sb = new StringBuilder();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            // 2. 排除表头 (判断 tradeTime 是否为数字)
            if (!isNumeric(fields[1])) return;
            try {
                // 3. 时间过滤
                // tradeTime 在 index 1, 格式如 092500 或 92500
                int time = Integer.parseInt(fields[1]);

                // 筛选区间: 09:29:57 (92957) 到 15:00:00 (150000)
                // 包含 92957 是为了计算 9:30:00 的 t-1 (3秒前) 的因子
                // tips: 我没设上限, 因为看到表里的数据都是在这个范围之下
                if (time >= 92957) {
                    sb.setLength(0); // 清空 StringBuilder
                    // 4. 字段提取与拼接
                    // [0] tradingDay
                    // [1] tradeTime
                    //sb.append(fields[0]).append(",").append(fields[1]);
                    sb.append(fields[1]);
                    // [12] tBidVol, [13] tAskVol
                    sb.append(",").append(fields[12]).append(",").append(fields[13]);

                    // [14] wBidPrc, [15] wAskPrc (保留用户要求的 VWAP)
                    sb.append(",").append(fields[14]).append(",").append(fields[15]);

                    // 提取 1-5 档 LOB 数据
                    // index 规律: bp1=17, bv1=18, ap1=19, av1=20
                    // bp_i = 17 + (i-1)*4
                    for (int i = 0; i < 5; i++) {
                        int base = 17 + (i * 4);
                        sb.append(",").append(fields[base]);     // bp
                        sb.append(",").append(fields[base + 1]); // bv
                        sb.append(",").append(fields[base + 2]); // ap
                        sb.append(",").append(fields[base + 3]); // av
                    }

                    // 输出: Key=Null (直接输出行), Value=清洗后的 CSV 行
                    // 也可以 Key=tradingDay+code 用于分区，但在 Map-Only 任务中 Key 主要是占位
                    outLine.set(sb.toString());
                    context.write(null, outLine);
                }
            } catch (Exception e) {
                // 忽略解析错误的行
            }
        }

        private boolean isNumeric(String str) {
            if (str == null || str.isEmpty()) return false;
            for (char c : str.toCharArray()) {
                if (!Character.isDigit(c)) return false;
            }
            return true;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Data PreProcessing");
        job.setJarByClass(FilterMapper.class);

        job.setMapperClass(ETLMapper.class);
        // 不需要 Reducer，直接由 Mapper 输出文件
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}