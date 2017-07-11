/**
 * Created by liam on 11/07/17.
 */
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageMark {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, DoubleWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //StringTokenizer itr = new StringTokenizer(value.toString());

            String[] tokens = value.toString().split("\n");
            for (String line : tokens) {
                String[] chunks = line.split("\\s+");
                String id = chunks[0];
                DoubleWritable mark =
                        new DoubleWritable(Double.parseDouble(chunks[2]));

                word.set(id);

                context.write(word, mark);
            }

//            while (itr.hasMoreTokens()) {
//                word.set(itr.nextToken());
//
//                if (first < 3) {
//                    System.out.print("HEY HEY HEY ITS FAT ALBERT!!!\n\n\n\n");
//                    System.out.print(word);
//                }
//                first += 1;
//
//                context.write(word, one);
//            }
        }
    }

    public static class IntSumReducer
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
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(AverageMark.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}