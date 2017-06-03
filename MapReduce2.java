//Finds the mutual friends of all friend pairs in the database.

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


public class Problem7 {
    /*
     * One possible type for the values that the mapper should output
     */
    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }
        
        public IntArrayWritable(IntWritable[] values) {
            super(IntWritable.class, values);
        }

        public int[] toIntArray() {
            Writable[] w = this.get();
            int[] a = new int[w.length];
            for (int i = 0; i < a.length; ++i) {
                a[i] = Integer.parseInt(w[i].toString());
            }
            return a;
        }
    }
    
      public static class MyMapper extends Mapper<Object, Text, Text, IntArrayWritable>
    { 
        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
        {
            
            
            String line = value.toString();
            if(line.contains(";")){
            String[] splitter = line.split(";");
            
            String[] idSplit = splitter[0].split(",");
            
            IntWritable userId = new IntWritable(Integer.parseInt(idSplit[0]));
            
            String[] fSplit = splitter[1].split(",");
            
            IntWritable[] fArray =  new IntWritable[fSplit.length];
            int index = 0;
            for(String val : fSplit)
            {
                IntWritable temp = new IntWritable(Integer.parseInt(val));
                fArray[index] = temp;
                index++;
            }
            
            
            for(IntWritable friend: fArray)
            {
                if(friend.compareTo(userId) < 0)
                {
                    String keyTemp = userId.toString() + "," + friend.toString();
                    context.write(new Text(keyTemp), new IntArrayWritable(fArray));
                }
                else
                {
                    String keyTemp = friend.toString() + "," + userId.toString();
                    context.write(new Text(keyTemp), new IntArrayWritable(fArray));
                }
            }
        }
      }
               
    }
      
      public static class MyReducer extends Reducer<Text, IntArrayWritable, Text, IntArrayWritable>
    {
        public void reduce(Text key, Iterable<IntArrayWritable> values, Context context)
            throws IOException, InterruptedException
        {
            
            Iterator<IntArrayWritable> iter = values.iterator();
            
            IntArrayWritable temp1 =  iter.next();
            IntArrayWritable temp2 = iter.next();
            
            int[] fp = temp1.toIntArray();
            int[] sp = temp2.toIntArray();
            
            IntWritable[] fp2 = new IntWritable[fp.length];
            int x = 0;
            IntWritable[] sp2 = new IntWritable[sp.length];
            int y = 0;
            
            for(int num: fp)
            {
                IntWritable numTemp = new IntWritable(num);
                fp2[x] = numTemp;
                x++;
                
            }
            
             for(int num: sp)
            {
                IntWritable numTemp = new IntWritable(num);
                sp2[y] = numTemp;
                y++;
                
            }
            
            HashSet<IntWritable> firstPair = new HashSet<IntWritable>();
            HashSet<IntWritable> secondPair = new HashSet<IntWritable>();
            
            for(int i = 0; i < fp2.length; i++)
            {
                firstPair.add(fp2[i]);
            }
            
            for(int j = 0; j < sp2.length; j++)
            {
                secondPair.add(sp2[j]);
            }
           
            if(secondPair.size() > firstPair.size())
            {
                firstPair.retainAll(secondPair);
                IntWritable[] intersection = firstPair.toArray(new IntWritable[firstPair.size()]);
                context.write(key, new IntArrayWritable(intersection));
             }
               
            
            else
            {
                secondPair.retainAll(firstPair);
                IntWritable[] intersection = secondPair.toArray(new IntWritable[firstPair.size()]);                
                context.write(key, new IntArrayWritable(intersection));
            }
            
            
        }
    }
    

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "problem 7");
        job.setJarByClass(Problem7.class);


        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntArrayWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntArrayWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
