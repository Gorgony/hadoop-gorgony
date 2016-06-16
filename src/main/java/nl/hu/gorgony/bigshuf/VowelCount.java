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
public class VowelCount {
    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(VowelCount.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(VowelCountMapper.class);
        job.setReducerClass(VowelCountReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        if (job.waitForCompletion(true)) {
            FileUtils.copyFile(new File(args[1]+ "/part-r-00000"),new File("english/VowelCount.lanstats"));
            FileUtils.deleteDirectory(new File(args[1]));
        }
    }
}

class VowelCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private String vowels = "aeiou";

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\\s");
        for (String s : words) {
            String purgedString = s.replaceAll("[^A-Za-z]+","");
            String lowerCasePurgedString = purgedString.toLowerCase();
            char[] charArray = lowerCasePurgedString.toCharArray();
            int total = 0;
            for(char c : charArray){
                if(vowels.contains(c + "")) total++;
            }
            if(charArray.length != 0){
                int percentage = (int) Math.round(total*100/purgedString.length());
                context.write(new Text("vowel"), new IntWritable(percentage));
            }
        }
    }
}

class VowelCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int amount = 0;
        for (IntWritable i : values) {
            sum += i.get();
            amount++;
        }
        int average = Math.round(sum/amount);
        context.write(key, new IntWritable(average));
    }
}


