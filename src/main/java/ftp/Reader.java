package ftp;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

public class Reader {

	public static void main(String[] args) throws IOException {
		FTPClient ftp = new FTPClient();
		// ftp.setFileType(FTP.ASCII_FILE_TYPE);
		// FTPClientConfig config = new FTPClientConfig();
		// config.set; // change required options
		// // for example config.setServerTimeZoneId("Pacific/Pitcairn")
		// ftp.configure(config);
		boolean error = false;
		try {
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

			// list files
			String dir = "/Users/zhouwei/tmp";
			FTPFile[] files = ftp.listFiles(dir);
			for (FTPFile e : files) {
				System.out.println(e.getType());
				String fileName = e.getName();
				System.out.println(fileName);
				String remote = dir + "/" + fileName;
				InputStream in = ftp.retrieveFileStream(remote);
				byte buffer[] = new byte[256];
				StringBuilder sb = new StringBuilder();
				try {
					while (in.read(buffer) != -1) {
						sb.append(new String(buffer));
					}
					System.out.println(sb.toString());
					System.out.println("reading " + remote + " done");
				} finally {
					in.close();
				}

			}

			// logout
			ftp.logout();
		} catch (IOException e) {
			error = true;
			e.printStackTrace();
		} finally {
			if (ftp.isConnected()) {
				try {
					ftp.disconnect();
				} catch (IOException ioe) {
					// do nothing
				}
			}
			System.exit(error ? 1 : 0);
		}
	}

}
