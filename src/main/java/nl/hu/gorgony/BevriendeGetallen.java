package nl.hu.gorgony;

/**
 * Created by njvan on 2/18/2016.
 */

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class BevriendeGetallen {

    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File("output/"));
        Job job = new Job();
        job.setJarByClass(CombinationCount.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(BevriendeGetallenMapper.class);
        job.setReducerClass(BevriendeGetallenReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Date start = new Date();
        if (job.waitForCompletion(true)) {
            Date end = new Date();
            long elapsed = end.getTime() - start.getTime();
            Logger l = Logger.getLogger(BevriendeGetallen.class.getName());
            l.log(Level.INFO, "Total time for mapreduce job -> " + elapsed);
        }
    }
}

class BevriendeGetallenMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        String[] getallen = value.toString().split("\\s");
        for (String s : getallen) {
            int getal1 = Integer.parseInt(s);
            int getal2 = 1;
            int stop = (int) (Math.sqrt(getal1)+1);
            int add = 2;
            if(getal1 % 2 == 0){
                add = 1;
                getal2 += 2;
                getal2 += (getal1/2);
            }
            for(int i = 3; i < stop; i += add){
                if(getal1 % i == 0){
                    getal2 += i;
                    int div = getal1/i;
                    if(div != i) {
                        getal2 += div;
                    }
                }
            }
            if((getal1 % 2) == (getal2 % 2)) {
                String output = (getal1 > getal2) ? getal2 + " " + getal1 : getal1 + " " + getal2;
                context.write(new Text(output), new IntWritable(1));
            }
        }
    }
}

class BevriendeGetallenReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int amount = 0;
        for (IntWritable i : values) {
            amount ++;
        }
        if(amount > 1){
            String[] getallen = key.toString().split(" ");
            context.write(new Text(getallen[0]), new IntWritable(Integer.parseInt(getallen[1])));
        }
    }
}

