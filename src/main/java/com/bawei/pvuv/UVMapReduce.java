/**   
 * Copyright © 2019 公司名. All rights reserved.
 * 
 * @Title: UVMapReduce.java 
 * @Prject: hdfsTest
 * @Package: com.bawei.pvuv 
 * @Description: TODO
 * @author: Lenovo   
 * @date: 2019年10月12日 上午9:54:25 
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
 * @ClassName: UVMapReduce 
 * @Description: TODO
 * @author: zxy
 * @date: 2019年10月12日 上午9:54:25  
 */
public class UVMapReduce {

	public static class UVMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		private Text k;
		private IntWritable v;
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			k.set(split[0]+"\001"+split[1]);
			context.write(k, v);
		}

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			k = new Text();
			v = new IntWritable(1);
			
		}
		
	}
	
	public static class UVReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text text, Iterable<IntWritable> iterable,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			context.write(text, new IntWritable(1));
		}
		
		
	}
}
