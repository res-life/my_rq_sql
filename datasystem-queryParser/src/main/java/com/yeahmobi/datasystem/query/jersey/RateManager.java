package com.yeahmobi.datasystem.query.jersey;

import java.util.concurrent.Semaphore;

public class RateManager {

	private static Semaphore semaphone = new Semaphore(3);

	private RateManager() {
	}

}
