package com.yumtao.hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class Test1 {
	private FileSystem fs = null;

	/**
	 * 递归获取文件。迭代器模式，需要时再查询，防止客户端内存溢出
	 */
	@Test
	public void getListByIterator() throws Exception {
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

		while (listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();
			long blockSize = fileStatus.getBlockSize();
			long accessTime = fileStatus.getAccessTime();
			System.out.println(String.format("filename : %s\t isDir : %b \t blockSize : %d \t accessTime: %s",
					fileStatus.getPath().getName(), fileStatus.isDirectory(), blockSize, accessTime));
			BlockLocation[] blockLocations = fileStatus.getBlockLocations();
			Arrays.asList(blockLocations).stream().forEach((BlockLocation location) -> {
				System.out
						.println(String.format("offset: %d \t length: %d", location.getOffset(), location.getLength()));
				try {
					Arrays.asList(location.getHosts()).stream().forEach(System.out::println);
				} catch (IOException e) {
				}
			});
		}
	}

	/**
	 * 获取指定目录下的文件，不包含递归
	 */
	@Test
	public void getList() throws FileNotFoundException, IOException {
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		Arrays.asList(listStatus).stream().forEach((FileStatus file) -> {
			long accessTime = file.getAccessTime();
			long blockSize = file.getBlockSize();
			String fileName = file.getPath().getName();
			System.out.println(String.format("filename : %s\t isDir : %b \t blockSize : %d \t accessTime: %s", fileName,
					file.isDirectory(), blockSize, accessTime));
		});
	}

	@Before
	public void init() throws Exception {
		Configuration conf = new Configuration();
		// conf.set("fs.defaultFS", "hdfs://master:9000");
		// fs = FileSystem.get(conf);
		fs = FileSystem.get(new URI("hdfs://master:9000"), conf, "root");
	}

	/**
	 * 通过流，上传文件
	 * 
	 * @throws Exception
	 */
	@Test
	public void uploadWithStream() throws Exception {
		FSDataOutputStream outputStream = fs.create(new Path("/qingqing.love"));
		InputStream inputStream = new FileInputStream("/Users/huangqikai/test.txt");
		IOUtils.copy(inputStream, outputStream);

	}

	/**
	 * hdfs 下载，指定文件长度下载（分片）
	 * 
	 * @throws Exception
	 */
	@Test
	public void splitByStream() throws Exception {
		FSDataInputStream inputStream = fs.open(new Path("/qingqing.love"));
		OutputStream outputStream = new FileOutputStream("/Users/huangqikai/qingqing.love");

		IOUtils.copyLarge(inputStream, outputStream, 5, 10);
	}

}
