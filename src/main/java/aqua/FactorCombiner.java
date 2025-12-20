package aqua;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FactorCombiner extends Reducer<Text, FactorAggWritable, Text, FactorAggWritable> {

    private final FactorAggWritable out = new FactorAggWritable();

    @Override
    protected void reduce(Text key, Iterable<FactorAggWritable> values, Context context)
            throws IOException, InterruptedException {

        out.clear();
        for (FactorAggWritable v : values) {
            out.add(v);
        }
        context.write(key, out);
    }
}