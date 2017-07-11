/**
 * Created by liam on 11/07/17.
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageMark {

    public static class AverageMarkMapper
            extends Mapper<Object, Text, Text, DoubleWritable>{

        private Text id = new Text();
        private DoubleWritable mark = new DoubleWritable(0.0);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] tokens = value.toString().split("\n");
            for (String line : tokens) {
                String[] chunks = line.split("\\s+");
                id.set(chunks[0]);
                mark.set(Double.parseDouble(chunks[2]));
                context.write(id, mark);
            }
        }
    }

    public static class AverageMarkReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0.0;
            int size = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                size++;
            }
            result.set(sum/size);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average mark");
        job.setJarByClass(AverageMark.class);
        job.setMapperClass(AverageMarkMapper.class);
        job.setCombinerClass(AverageMarkReducer.class);
        job.setReducerClass(AverageMarkReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}