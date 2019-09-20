package com.vikas.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;


public class WordCount extends Configured implements Tool {
	
	public static class MapClass extends
			Mapper<Object, Text, Text, IntWritable> {

		private static final IntWritable ONE = new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Configuration con= context.getConfiguration();
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				word.set(token);
				context.write(word, ONE);
			}
		}
	}
		
	public static class Reduce extends
			Reducer<Text, IntWritable, Text, MapWritable> {
		private IntWritable count = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			count.set(sum);
			MapWritable mapWritable = new MapWritable();
			mapWritable.put(new Text("sum"), new IntWritable(sum));
			mapWritable.put(new Text("key"), key);
			context.write(key, mapWritable);
		}
	}

	public int run(String[] arg0) throws Exception {

		Configuration conf = new Configuration();
		conf.set("es.nodes","localhost:9200");
		conf.set("es.resource", "my_index/words");
		Job job = new Job(conf);
		job.setOutputFormatClass(EsOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);

		FileInputFormat.setInputPaths(job, new Path("data/input"));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1; 
	}
}
