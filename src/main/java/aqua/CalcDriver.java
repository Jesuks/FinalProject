package aqua;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat; // 新增导入
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; // 新增导入

public class CalcDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        conf.set("mapreduce.output.textoutputformat.separator", "");
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

        // ====== 推荐新增：本地模式并发上限（如果日志是 job_local... 非常关键）======
        int cores = Runtime.getRuntime().availableProcessors();
        conf.setInt("mapreduce.local.map.tasks.maximum", cores);
        conf.setInt("mapreduce.local.reduce.tasks.maximum", Math.max(1, cores / 2));

        // ====== 推荐新增：shuffle 优化（低风险）======
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        conf.setInt("mapreduce.task.io.sort.mb", 256); // 内存够可再加大到 384/512 试
        conf.setInt("mapreduce.task.io.sort.factor", 100);

        Job job = Job.getInstance(conf, "Stock Factor Calculation");
        job.setJarByClass(CalcDriver.class);

        job.setMapperClass(FactorMapper.class);
        job.setReducerClass(FactorReducer.class);

        // ★★★ 必须保持 1 个 Reducer ★★★
        // 这样才能保证同一天的数据按顺序进入同一个 reducer，从而正确触发写表头的逻辑
        job.setPartitionerClass(TradingDayPartitioner.class);
        job.setNumReduceTasks(conf.getInt("aqua.reducers", 8));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // ====== 推荐新增：合并小文件，减少 mapper 数量 ======
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 每个 split 目标 64MB（小文件多时非常有效）
        CombineTextInputFormat.setMaxInputSplitSize(job, 64L * 1024 * 1024);
        CombineTextInputFormat.setMinInputSplitSize(job, 32L * 1024 * 1024);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // ★★★ 新增配置：使用 LazyOutputFormat ★★★
        // 这行代码的作用是：只有当确实有数据写入时，才创建文件。
        // 防止在输出目录的根目录下生成一个空的 part-r-00000 文件
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}