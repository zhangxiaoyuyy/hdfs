/**   
 * Copyright © 2019 公司名. All rights reserved.
 * 
 * @Title: UVApp.java 
 * @Prject: hdfsTest
 * @Package: com.bawei.pvuv 
 * @Description: TODO
 * @author: Lenovo   
 * @date: 2019年10月12日 上午11:03:21 
 * @version: V1.0   
 */
package com.bawei.pvuv;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bawei.pvuv.UVMapReduce.UVMapper;
import com.bawei.pvuv.UVMapReduce.UVReducer;
import com.bawei.pvuv.UVMapReduceTwo.UVStepTwoMapper;
import com.bawei.pvuv.UVMapReduceTwo.UVStepTwoReduce;

/**
 * @ClassName: UVApp
 * @Description: TODO
 * @author: zxy
 * @date: 2019年10月12日 上午11:03:21
 */
public class UVApp {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance();
		
		job.setJobName("UVStatic");
		job.setJarByClass(UVApp.class);
		
		job.setMapperClass(UVMapper.class);
		job.setReducerClass(UVReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(""));
		FileOutputFormat.setOutputPath(job, new Path(""));
		
		if(job.waitForCompletion(true)) {
			Job job2 = Job.getInstance();
			
			job2.setJobName("UVStatic");
			job2.setJarByClass(UVApp.class);
			
			job2.setMapperClass(UVStepTwoMapper.class);
			job2.setReducerClass(UVStepTwoReduce.class);
			
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job2, new Path(""));
			FileOutputFormat.setOutputPath(job2, new Path(""));
			
			job2.waitForCompletion(true);
		}
		
	}

}
