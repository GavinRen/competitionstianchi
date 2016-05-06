package processdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by bigdata on 16-4-20.
 */
public class PredictedData {
    public static class predicteMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text keyout = new Text();
        private IntWritable valueout = new IntWritable();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            int datetable = 0;

            String date_str = null;
            String[] rec = line.split("\t");
            if (rec.length == 4) {
                date_str = rec[2].trim();
                if (date_str != null) {
                    datetable = Integer.parseInt(date_str);

                    if (0 < datetable && datetable <= 3) {
                        keyout.set(rec[0] +"\t" + rec[1]);
                        int sum = Integer.parseInt(rec[3].trim());
                        valueout.set(sum);
                        context.write(keyout, valueout);
                    }
                }
            }
        }
    }

    public static class predicteReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
        private FloatWritable valueout = new FloatWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
                count++;
            }
            float sumf = sum;
            float average = sumf / count;
            valueout.set(average);
            context.write(key, valueout);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Path input2 = new Path("hdfs://bigdata-server:8020/test/tianchidata/sumoutput/");
        Path output2 = new Path("hdfs://bigdata-server:8020/test/tianchidata/1predictedoutput/");
        FileSystem fs = output2.getFileSystem(conf);
        if (fs.exists(output2)) {
            fs.delete(output2);
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(PredictedData.class);
        job.setJobName("predict data");

        job.setMapperClass(predicteMapper.class);
        job.setReducerClass(predicteReducer.class);
        //job.setCombinerClass(ConcersListReducer.class);

        FileInputFormat.addInputPath(job, input2);
        FileOutputFormat.setOutputPath(job, output2);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
