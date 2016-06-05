package ftp;

import java.io.IOException;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

public class Reader {

	public static void main(String[] args) {
		FTPClient ftp = new FTPClient();
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
			FTPFile[] files = ftp.listFiles("/Users/zhouwei");
			for (FTPFile e : files) {
				System.out.println(e.getName());
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
