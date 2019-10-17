/**   
 * Copyright © 2019 公司名. All rights reserved.
 * 
 * @Title: PVMapReduce.java 
 * @Prject: hdfsTest
 * @Package: com.bawei.pvuv 
 * @Description: TODO
 * @author: Lenovo   
 * @date: 2019年10月12日 上午8:07:48 
 * @version: V1.0   
 */
package com.bawei.pvuv;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** 
 * @ClassName: PVMapReduce 
 * @Description: TODO
 * @author: zxy
 * @date: 2019年10月12日 上午8:07:48  
 */
public class PVMapReduce{

	public static class PVMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		private Text k;
		private IntWritable v;
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			k = new Text();
			v = new IntWritable(1);
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			
			k.set(split[1]);
			context.write(k, v);
		}
	}
	
	public static class PVReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> value,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			Iterator<IntWritable> iterator = value.iterator();
			int count= 0;
			while(iterator.hasNext()) {
				
				IntWritable next = iterator.next();
				
				count = count+next.get();
			}
			context.write(key, new IntWritable(count));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance();
		
		job.setJobName("PVStatic");
		job.setJarByClass(PVMapReduce.class);
		
		job.setMapperClass(PVMapper.class);
		
		job.setReducerClass(PVReducer.class);
		
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("C:\\Users\\Lenovo\\Desktop\\input"));
		FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\Lenovo\\Desktop\\output"));
		
		job.waitForCompletion(true);
		
	}
	/**
	 * 没用
	 * @ClassName: submaitLocalToYarn 
	 * @Description: TODO
	 * @author: zxy
	 * @date: 2019年10月12日 上午9:53:23
	 */
	public static class submaitLocalToYarn {
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			//伪装用户为root
            System.setProperty("HADOOP_USER_NAME","root");


            Configuration conf = new Configuration();
            conf.set("fs.defaultFS","hdfs://hdp1:8020");
            conf.set("yarn.resourcemanager.hostname","hdp4");
            conf.set("mapreduce.framework.name","yarn");
            conf.set("mapreduce.app-submission.cross-platform","true");

            Job job = Job.getInstance(conf);


            job.setJobName("PVStatic");
            job.setJar("C:\\Users\\wangy\\IdeaProjects\\newdemofor1704e\\hdfstest\\target\\hdfstest-1.0-SNAPSHOT.jar");


            //设置mapper的执行类
            job.setMapperClass(PVMapper.class);
            //设置reducer的执行类
            job.setReducerClass(PVReducer.class);
            //mapper输出的key的类型，在mapper输出类型与之相同的时候，可省略
            //job.setMapOutputKeyClass(Text.class);
            //mapper输出的value的类型，在reducer输出类型与之相同的时候，可省略
            //job.setMapOutputValueClass(IntWritable.class);


            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);


            FileInputFormat.addInputPath(job, new Path("/PVUV/input"));
            FileOutputFormat.setOutputPath(job, new Path("/PVUV/output"));
            //提交任务，等待执行完成
            job.waitForCompletion(true);
		}
	}
}
