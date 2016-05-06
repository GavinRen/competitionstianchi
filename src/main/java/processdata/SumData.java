package processdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by bigdata on 16-4-19.
 */
public class SumData {
    public static class SumDataMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text keyout = new Text();
        private IntWritable valueout = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] rec = line.split("\t");
            keyout.set(rec[0] + "\t" + rec[1] + "\t" + rec[2]);
            valueout.set(Integer.valueOf(rec[3].trim()));
            context.write(keyout, valueout);
        }
    }

    public static class SumDataReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Path input1 = new Path("hdfs://master:8020/test/tianchidata/output/");
        Path output1 = new Path("hdfs://master:8020/test/tianchidata/sumoutput/");
        FileSystem fs = output1.getFileSystem(conf);
        if (fs.exists(output1)) {
            fs.delete(output1);
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(SumData.class);
        job.setJobName("sum data");

        job.setMapperClass(SumDataMapper.class);
        job.setReducerClass(SumDataReducer.class);
        //job.setCombinerClass(ConcersListReducer.class);

        FileInputFormat.addInputPath(job, input1);
        FileOutputFormat.setOutputPath(job, output1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
