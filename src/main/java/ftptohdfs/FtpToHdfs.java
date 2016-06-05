package ftptohdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.net.URI;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FtpToHdfs {

	static FTPClient ftp;
	static FileSystem fs;

	public static void main(String[] args) throws IOException {
		try {
			init();

			exec();

		} finally {
			destroy();
		}
	}

	static void exec() throws IOException {
		InputStream in = getInputStream(Config.FTP_FILE_PATH);
		Path outFile = new Path(Config.HDFS_FILE_PATH);

		// Check if output is valid
		if (fs.exists(outFile)) {
			throw new RuntimeException("Output already exists");
		}

		// Write to the new file
		FSDataOutputStream out = fs.create(outFile);
		try {
			write(in, out);
		} finally {
			try {
				in.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			out.close();
		}
	}

	static void init() throws SocketException, IOException {
		initFtp();
		initHdfs();
	}

	static void initFtp() throws SocketException, IOException {
		ftp = new FTPClient();
		// ftp.setFileType(FTP.ASCII_FILE_TYPE);
		// FTPClientConfig config = new FTPClientConfig();
		// config.set; // change required options
		// // for example config.setServerTimeZoneId("Pacific/Pitcairn")
		// ftp.configure(config);
		int reply;
		String server = "localhost";
		ftp.connect(server);
		System.out.println("Connected to " + server + ".");
		System.out.print(ftp.getReplyString());
		String username = "zhouwei";
		boolean loginOk = ftp.login(username, "helloworld42");
		if (!loginOk) {
			System.err.println("failed to login " + server + " with username " + username);
		}

		// After connection attempt, you should check the reply code to
		// verify
		// success.
		reply = ftp.getReplyCode();

		if (!FTPReply.isPositiveCompletion(reply)) {
			ftp.disconnect();
			System.err.println("FTP server refused connection.");
			System.exit(1);
		}
	}

	static void initHdfs() throws IOException {
		Configuration conf = new Configuration();
		fs = FileSystem.get(URI.create(Config.HDFS_FILE_PATH), conf);
	}

	static void destroy() {
		logoutFtp();
		disconnectFtp();
		closeHdfs();
	}

	static void logoutFtp() {
		if (ftp == null) {
			return;
		}

		try {
			ftp.logout();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	static void disconnectFtp() {
		if (ftp == null) {
			return;
		}

		if (ftp.isConnected()) {
			try {
				ftp.disconnect();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	static InputStream getInputStream(String remote) throws IOException {
		return ftp.retrieveFileStream(remote);
	}

	static void write(InputStream in, OutputStream out) throws IOException {
		byte buffer[] = new byte[256];
		int bytesRead = 0;
		while ((bytesRead = in.read(buffer)) != -1) {
			String str = new String(buffer);
			System.out.println(str);
			out.write(buffer, 0, bytesRead);
		}
	}

	static void closeHdfs() {
		try {
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
