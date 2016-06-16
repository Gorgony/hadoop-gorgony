package nl.hu.gorgony.bigshuf;

import org.apache.commons.io.FileUtils;
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

import java.io.File;
import java.io.IOException;

/**
 * Created by njvan on 14-Jun-16.
 */
public class StartEndCount {
    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(nl.hu.gorgony.bigshuf.StartEndCount.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(nl.hu.gorgony.bigshuf.StartEndCountMapper.class);
        job.setReducerClass(nl.hu.gorgony.bigshuf.StartEndCountReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        if (job.waitForCompletion(true)) {
            FileUtils.copyFile(new File(args[1] + "/part-r-00000"), new File("english/StartEndCount.lanstats"));
            FileUtils.deleteDirectory(new File(args[1]));
        }
    }
}


class StartEndCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\\s");
        for (String s : words) {
            String purgedString = s.replaceAll("[^A-Za-z]+", "");
            String lowerCasePurgedString = purgedString.toLowerCase();
            if(lowerCasePurgedString.length() > 0) {
                context.write(new Text("start:" + lowerCasePurgedString.charAt(0)), new IntWritable(1));
                context.write(new Text("end:" + lowerCasePurgedString.charAt(lowerCasePurgedString.length() - 1)), new IntWritable(1));
            }
        }
    }
}

class StartEndCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int amount = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        context.write(key, new IntWritable(sum));
    }
}



