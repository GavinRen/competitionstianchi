package processdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
 * Created by bigdata on 16-4-22.
 */
public class CountID {
    public static class CountIDmapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private Text valueout = new Text();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] rec = line.split(",");
            String id = rec[1];
            if (id != null) {
                valueout.set(id);
                context.write(valueout, NullWritable.get());
            }

        }
    }
    public static class CountIDreducer extends Reducer<Text,NullWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Path input = new Path("hdfs://master:8020/test/tianchidata/input/");
        Path output = new Path("hdfs://master:8020/test/tianchidata/countId");
        FileSystem fs = output.getFileSystem(conf);
        if (fs.exists(output)) {
            fs.delete(output);
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(CountID.class);
        job.setJobName("count itemId");

        job.setMapperClass(CountIDmapper.class);
        job.setReducerClass(CountIDreducer.class);
        //job.setCombinerClass(ConcersListReducer.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
