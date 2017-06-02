// Accepts input from text file of users in a social media database
// User data in the following format:
// Age,Last,First,DOB (YYYY-MM-DD,email,User Groups;Friends
// This program finds the number of users each email domain has

import java.io.IOException;
import java.time.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Problem4 {
    
    public static class MyMapper extends Mapper<Object, Text, Text, LongWritable>
    {
        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
        {
            /*Converts Text input to string, checks to see which element
             * has the @ character.
            */
            String line = value.toString();
            
            String[] elements = line.split(",");
            
            CharSequence symbol = "@";
            
            for(int i = 0; i < elements.length; i++)
            {
                if(elements[i].contains(symbol))
                {
                    String[] addressTemp = elements[i].split("@");
                    //Removes the @ to only store the email domain
                    String a = addressTemp[1];
                    String[] addressList = a.split(";");
                    String address = addressList[0];
                    
                    context.write(new Text(address), new LongWritable(1));
                }
            }
        }
    }
    
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>
    {
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException
        {
            //Adds the number of instances for each domain
            long i = 0;
            for(LongWritable val : values)
            {
                i += val.get();
            }
            
            context.write(key, new LongWritable(i));
            
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //Gets text file containing user data
        Job job = Job.getInstance(conf, "users");

        // Specifies the name of the outer class.
        job.setJarByClass(Problem4.class);

        // Specifies the names of the mapper and reducer classes.
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Sets the type for the keys output by the mapper and reducer.
        job.setOutputKeyClass(Text.class);

        // Sets the type for the values output by the mapper and reducer,
        job.setOutputValueClass(LongWritable.class);

        // Sets the type for the keys output by the mapper.
        job.setMapOutputKeyClass(Text.class);

        // Sets the type for the values output by the mapper.
        job.setMapOutputValueClass(LongWritable.class);


        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
