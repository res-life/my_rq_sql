package com.yeahmobi.datasystem.query.akka.http;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

public class AyncInputStream extends InputStream {

	private Object lockObject = new Object();

	private int index = 0;
	private List<Byte> byteArray = new LinkedList<>();

	private volatile boolean isFinished = false;

	public void setFinished() {
		isFinished = true;
	}

	public void write(byte[] bytes) {
		if (null == bytes) {
			return;
		}

		synchronized (byteArray) {
			for (int i = 0; i < bytes.length; ++i) {
				byteArray.add(bytes[i]);
			}
		}
		synchronized (lockObject) {
			lockObject.notifyAll();
		}
	}

	@Override
	public int read() throws IOException {

		if (isFinished) {
			return readWhenFinished();
		} else {
			synchronized (byteArray) {
				if (index < byteArray.size()) {
					return byteArray.get(index++);
				}
			}
		}

		while (!canRead()) {
			try {
				synchronized (lockObject) {
					lockObject.wait(3600 * 1000);
				}

			} catch (InterruptedException e) {
				throw new IOException(
						"Error occured: interruped exception when read data from aync input stream, already waited for 1 hour",
						e);
			}
		}
		return read();
	}

	private boolean canRead() {
		boolean isAvailable = false;
		synchronized (byteArray) {
			isAvailable = index < byteArray.size();
		}

		return isFinished || isAvailable;
	}

	private int readWhenFinished() {
		synchronized (byteArray) {
			if (index == byteArray.size()) {
				return -1;
			} else {
				return byteArray.get(index++);
			}
		}
	}

	@Override
	public void close() throws IOException {
		byteArray.clear();
	}
}
