package hdfs;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsFileWriter {

	static void printAndExit(String str) {
		System.err.println(str);
		System.exit(1);
	}

	public static void main(String[] argv) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(Config.URI), conf);

		// Hadoop DFS deals with Path
		Path outFile = new Path(Config.URI);

		// Check if output is valid
		if (fs.exists(outFile)) {
			printAndExit("Output already exists");
		}

		// Write to the new file
		String text = "Hello from HdfsFileWriter!";
		InputStream in = new ByteArrayInputStream(text.getBytes());
		FSDataOutputStream out = fs.create(outFile);
		byte buffer[] = new byte[256];
		try {
			int bytesRead = 0;
			while ((bytesRead = in.read(buffer)) != -1) {
				out.write(buffer, 0, bytesRead);
			}
			System.out.println("writing " + Config.URI + " done");
		} catch (IOException e) {
			System.out.println("Error while writing file");
		} finally {
			out.close();
		}
	}
}