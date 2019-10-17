/**   
 * Copyright © 2019 公司名. All rights reserved.
 * 
 * @Title: UVMapReduceTwo.java 
 * @Prject: hdfsTest
 * @Package: com.bawei.pvuv 
 * @Description: TODO
 * @author: Lenovo   
 * @date: 2019年10月12日 上午10:03:01 
 * @version: V1.0   
 */
package com.bawei.pvuv;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/** 
 * @ClassName: UVMapReduceTwo 
 * @Description: TODO
 * @author: zxy
 * @date: 2019年10月12日 上午10:03:01  
 */
public class UVMapReduceTwo {

	public static class UVStepTwoMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] arr = value.toString().split("\t");
			String[] split = arr[0].split("\\001");
			
			String userIp = split[0];
			String url = split[1];
			context.write(new Text(url) ,new IntWritable(1) );
		}

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}
	}
	
	public static class UVStepTwoReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text arg0, Iterable<IntWritable> arg1,
				Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
			
			int count = 0;
			for (IntWritable intWritable : arg1) {
				int i = intWritable.get();
				count = count + i;
			}
			arg2.write(arg0, new IntWritable(count));
		}
	}
}
