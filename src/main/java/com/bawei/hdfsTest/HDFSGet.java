/**   
 * Copyright © 2019 公司名. All rights reserved.
 * 
 * @Title: HDFSGet.java 
 * @Prject: hdfsTest
 * @Package: com.bawei.hdfsTest 
 * @Description: TODO
 * @author: Lenovo   
 * @date: 2019年10月9日 下午8:20:22 
 * @version: V1.0   
 */
package com.bawei.hdfsTest;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

/** 
 * @ClassName: HDFSGet 
 * @Description: TODO
 * @author: zxy
 * @date: 2019年10月9日 下午8:20:22  
 */
public class HDFSGet {

	@Test
	public void putTest() throws IOException, InterruptedException {
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.56.104:8020"), conf, "root");

		FSDataOutputStream out = fs.create(new Path("/test/input/aa.txt"));

		FileInputStream in = new FileInputStream("C:\\Users\\Lenovo\\Desktop\\input\\aa.txt");

		IOUtils.copyBytes(in, out, 1024, true);

	}
}
