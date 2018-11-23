/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Date;
import java.util.Random;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCountFlorin extends Configured implements Tool{
  
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable();
    private Text word = new Text();
      
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         StringTokenizer itr = new StringTokenizer(value.toString());
         int flopsMapper = context.getConfiguration().getInt("wordcountflorin.flopsMapper", 0);
         System.out.println(new Date() + " PAMELA before cycles in flopsMapper " + flopsMapper);
         while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            Random r = new Random();
            int sum2 = 0;
            for (int i = 1; i <= flopsMapper; i++) {
               sum2 += r.nextFloat() * r.nextFloat();
            }

            one.set(sum2 % 100);
            context.write(word, one);
         }
      }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
      public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
         int sum = 0;

         for (IntWritable val : values) {
            sum += val.get();
         }
         
         int flopsReducer = context.getConfiguration().getInt("wordcountflorin.flopsReducer", 0);
         System.out.println(new Date() + " PAMELA before cycles in flopsMapper " + flopsReducer + " ignoring sum " + sum);
         Random r = new Random();
         int sum2 = 0;
         for (int i = 1; i <= flopsReducer; i++) {
            sum2 += r.nextFloat() * r.nextFloat();
         }
         result.set(sum2 % 100);
         context.write(key, result);
      }
   }


  public int run(String[] args) throws Exception {
    if (args.length < 4) {
      System.err.println("Usage: wordcountflorin <in> [<in>...] <out> FLOPSMApCall FLOPSRedCall");
      System.exit(2);
    }
    
    System.out.println(new Date() + " PAMELA setting flopsMapper in config " + args[2] + " flopsReducer " + args[3]);

    Configuration conf = getConf();
    conf.setInt("wordcountflorin.flopsMapper", Integer.valueOf(args[2]));
    conf.setInt("wordcountflorin.flopsReducer", Integer.valueOf(args[3]));
    Job job = Job.getInstance(conf, "wordcountflorin");
    job.setJarByClass(WordCountFlorin.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPaths(job, args[0]);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
   
    return job.waitForCompletion(true) ? 0 : 1;
   
  }
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new WordCountFlorin(), args);
    System.exit(ret);
  }

}