package processdata;

import datautils.DataUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.text.ParseException;

/**
 * Created by renguifu on 16-4-19.
 */
public class ExtractData  {
    public static class extractdataMapper extends Mapper<LongWritable,Text,Text,Text>{
        private Text keyout = new Text();
        private Text valueout= new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line =value.toString();
            DataUtils data =new DataUtils();
            data=DataUtils.processData(line);
            if (data.getItemID()!=null&&
                    data.getDate()!=null&&
                    data.getWarehouseCode()!=null
                    &&data.getSaleNumber()!=null){
                keyout.set(data.getItemID()+"\t"+data.getWarehouseCode());
                valueout.set(data.getDate()+"\t"+data.getSaleNumber());

                context.write(keyout,valueout);
            }
        }
    }
    public static class extractdataReducer extends Reducer<Text,Text,Text,Text>{
        private Text valueout =new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value :values){
                String line =value.toString().trim();
                String [] rec=line.split("\t");
                String date=rec[0];
                if (date.equals("20141111")||
                        date.equals("20141212")||
                        date.equals("20151111")||
                        date.equals("20151212")){

                }else {
                    try {
                        int lable = DataUtils.setlable(date.trim());
                        String lable_str=String.valueOf(lable);
                        valueout.set(lable_str+"\t"+rec[1]);
                        context.write(key,valueout);
                    }catch (ParseException e){
                        System.out.println("日期格式不对");
                    }
                }


            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Path input = new Path("hdfs://master:8020/test/tianchidata/input/");
        Path output = new Path("hdfs://master:8020/test/tianchidata/output");
        FileSystem fs = output.getFileSystem(conf);
        if (fs.exists(output)) {
            fs.delete(output);
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(ExtractData.class);
        job.setJobName("ExtractData form data");

        job.setMapperClass(extractdataMapper.class);
        job.setReducerClass(extractdataReducer.class);
        //job.setCombinerClass(ConcersListReducer.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
