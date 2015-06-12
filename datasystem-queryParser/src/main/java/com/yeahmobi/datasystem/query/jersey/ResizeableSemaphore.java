package com.yeahmobi.datasystem.query.jersey;

import java.util.concurrent.Semaphore;

public class ResizeableSemaphore {

	private static final ResizeableSemaphore instance = new ResizeableSemaphore();
	private Semaphore semaphone = new Semaphore(3);

	public static ResizeableSemaphore getInstance() {
		return instance;
	}
}
