package nl.hu.gorgony.bigshuf;

/**
 * Created by njvan on 2/18/2016.
 */

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
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CombinationCount {

    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(CombinationCount.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CombinationCountMapper.class);
        job.setReducerClass(CombinationCountReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        if (job.waitForCompletion(true)) {
            FileUtils.copyFile(new File(args[1]+ "/part-r-00000"),new File("english/CombinationCount.lanstats"));
            FileUtils.deleteDirectory(new File(args[1]));
        }
    }
}

class CombinationCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\\s");
        for (String s : words) {
            String purgedString = s.replaceAll("[^A-Za-z]+","");
            String lowerCasePurgedString = purgedString.toLowerCase();
            char[] charArray = lowerCasePurgedString.toCharArray();
            for(int i = 0; i < (charArray.length - 1); i++){
                context.write(new Text("" + charArray[i] + charArray[i+1]), new IntWritable(1));
            }
        }
    }
}

class CombinationCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

