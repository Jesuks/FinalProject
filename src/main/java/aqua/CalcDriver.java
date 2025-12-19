package aqua;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CalcDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

        // ====== 保持你原来的优化配置不变 ======
        int cores = Runtime.getRuntime().availableProcessors();
        conf.setInt("mapreduce.local.map.tasks.maximum", cores);
        conf.setInt("mapreduce.local.reduce.tasks.maximum", Math.max(1, cores / 2));

        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        conf.setInt("mapreduce.task.io.sort.mb", 256);
        conf.setInt("mapreduce.task.io.sort.factor", 100);

        Job job = Job.getInstance(conf, "Stock Factor Calculation");
        job.setJarByClass(CalcDriver.class);

        job.setMapperClass(FactorMapper.class);
        job.setReducerClass(FactorReducer.class);

        job.setPartitionerClass(TradingDayPartitioner.class);
        job.setNumReduceTasks(conf.getInt("aqua.reducers", 8));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 64L * 1024 * 1024);
        CombineTextInputFormat.setMinInputSplitSize(job, 32L * 1024 * 1024);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);

        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        // 1. 执行任务
        boolean result = job.waitForCompletion(true);

        // 2. ★ 新增逻辑：任务成功后立即在 Java 中处理文件名 ★
        if (result) {
            FileSystem fs = FileSystem.get(conf);
            // 遍历输出目录下的所有文件
            FileStatus[] statuses = fs.listStatus(outputPath);
            for (FileStatus status : statuses) {
                String fileName = status.getPath().getName();

                // 如果文件名包含 .csv-r- (MultipleOutputs 生成的带编号文件名)
                if (fileName.contains(".csv-r-")) {
                    // 按照 "-r-" 分割，只取前半部分 (即 0102.csv)
                    String newName = fileName.split("-r-")[0];
                    Path oldPath = status.getPath();
                    Path newPath = new Path(outputPath, newName);

                    // 重命名：将 0102.csv-r-00000 改为 0102.csv
                    if (!fs.exists(newPath)) {
                        fs.rename(oldPath, newPath);
                    }
                }
                // 可选：删除掉产生的 _SUCCESS 文件，让目录只剩下 .csv
                else if (fileName.startsWith("_SUCCESS")) {
                    fs.delete(status.getPath(), false);
                }
            }
        }

        System.exit(result ? 0 : 1);
    }
}