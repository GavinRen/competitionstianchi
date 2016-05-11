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
    public static class predicteMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text keyout = new Text();
        private Text valueout = new Text();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            int count = 0;
            int datetable = 0;
            String[] date_num = {"1", "2", "3", "4", "5","6","7","8"};

            String date_str = null;
            String[] rec = line.split("\t");
            if (rec.length == 4) {
                date_str = rec[2].trim();
                if (date_str != null) {
                    for (int i = 0; i < date_num.length; i++) {
                        if (date_str.equals(date_num[i])) {
                            keyout.set(rec[0] + "\t" + rec[1]);
                            valueout.set(date_str + "\t" + rec[3].trim());
                            context.write(keyout,valueout);
                        }
                    }
                }
                /*if (date_str != null) {
                    datetable = Integer.parseInt(date_str);
*//*
                    if (datetable == 1) {
                        keyout.set(rec[0] +"\t" + rec[1]);
                        int sum = Integer.parseInt(rec[3].trim());
                        valueout.set(sum);
                        context.write(keyout, valueout);
                    }*//*
                    if (datetable == 1) {
                        keyout.set(rec[0] +"\t" + rec[1]);
                        valueout.set("1"+"\t"+rec[3].trim());
                        context.write(keyout,valueout);
                    }else {
                        if (datetable==2){
                            keyout.set(rec[0] +"\t" + rec[1]);
                            valueout.set("2"+"\t"+rec[3].trim());
                            context.write(keyout,valueout);

                        }else {
                            if (datetable==3){
                                keyout.set(rec[0] +"\t" + rec[1]);
                                valueout.set("3"+"\t"+rec[3].trim());
                                context.write(keyout,valueout);
                            }
                        }
                    }
*//*
                    if (0 < datetable && datetable <= 3) {
                        keyout.set(rec[0] +"\t" + rec[1]);
                        int sum = Integer.parseInt(rec[3].trim());
                        valueout.set(sum);
                        context.write(keyout, valueout);
                    }*//*
                }*/
            }
        }
    }

    public static class predicteReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private DoubleWritable valueout = new DoubleWritable();
        private String[] month_num = {"1", "2", "3", "4", "5","6","7","8"};
        private double a = 0.6;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int[] value_num = {0, 0, 0, 0, 0, 0,0,0,0};
            for (Text value : values) {
                String valuestr = value.toString().trim();
                String[] rec = valuestr.split("\t");
                for (int i = 0; i < month_num.length; i++) {
                    if (rec[0].trim().equals(month_num[i])) {
                        int num = Integer.parseInt(month_num[i].trim());
                        value_num[num] = Integer.parseInt(rec[1].trim());
                    }
                }
            }
            double predictedvalue = 0;
            for (int i = 1; i < value_num.length; i++) {
                predictedvalue += a * Math.pow(1 - a, i - 1) * value_num[i];
            }
            valueout.set(predictedvalue);
            context.write(key, valueout);
            /*int count = 0;
            int value1=0;
            int value2=0;
            int value3=0;
            for (Text value : values) {
               String valuestr= value.toString().trim();
                String[] rec =valuestr.split("\t");
                if (rec.length==2){
                    if (rec[0].trim().equals("1")){
                        value1=Integer.parseInt(rec[1].trim());
                    }else {
                        if (rec[0].trim().equals("2")){
                          value2=Integer.parseInt(rec[1].trim());
                        }else {
                            if (rec[0].trim().equals("3")){
                              value3=Integer.parseInt(rec[1].trim());
                            }
                        }
                    }
                }
                count++;
            }

            double average = 0.4*value1+0.35*value2+0.25*value3;
            valueout.set(average);
            context.write(key, valueout);
        }*/
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Path input2 = new Path("hdfs://bigdata-server:8020/test/tianchidata/sumoutput/");
        Path output2 = new Path("hdfs://bigdata-server:8020/test/tianchidata/predictedoutput5/");
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
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

