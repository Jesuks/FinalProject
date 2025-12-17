package aqua;

import org.apache.hadoop.conf.Configuration;
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
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

        Job job = Job.getInstance(conf, "Stock Factor Calculation");
        job.setJarByClass(CalcDriver.class);

        job.setMapperClass(FactorMapper.class);
        job.setReducerClass(FactorReducer.class);

        // ★★★ 必须保持 1 个 Reducer ★★★
        // 这样才能保证同一天的数据按顺序进入同一个 reducer，从而正确触发写表头的逻辑
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

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