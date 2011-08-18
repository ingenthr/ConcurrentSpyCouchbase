/*
 * Copyright (C) 2011 Couchbase, Inc.
 * All rights reserved.
 */
package com.couchbase.test.concspy;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.MemcachedClient;

/**
 *
 * @author ingenthr
 */
public class SleepingTester implements Runnable {

	MemcachedClient mc = null;
	final int times;
	private int getSuccess, getFailure, setSuccess, setFailure;

	public SleepingTester(MemcachedClient client, int iterations) {
		mc = client;
		times = iterations;

	}

	@Override
	public void run() {
		String threadName = Thread.currentThread().getName();
		getSuccess = 0;
		getFailure = 0;
		setSuccess = 0;
		setFailure = 0;

		Logger.getLogger(SleepingTester.class.getName()).log(Level.INFO,
						"Thread {0} starting.", threadName);
		for (int i = 0; i < times; i++) {
			try {
				runSets(threadName);
				Thread.sleep(1000);
				runGets(threadName);
				Thread.sleep(3000);
			} catch (InterruptedException ex) {
				Logger.getLogger(SleepingTester.class.getName()).log(Level.WARNING,
								"Thread waiting to do more terminated.", ex);
			} catch (ExecutionException ex) {
				Logger.getLogger(SleepingTester.class.getName()).log(Level.SEVERE,
								"Thread trying to work hit a problem, trying to continue.", ex);
			} catch (Exception e) {
				Logger.getLogger(SleepingTester.class.getName()).log(Level.WARNING,
								"Unexpected Exception in the main run() for loop", e);
				getFailure++;
			}
		}
		Logger.getLogger(SleepingTester.class.getName()).log(Level.INFO,
						"Thread {0} completed all of it's sets and gets. "
						+ "Get successes: " + getSuccess + "; get failures: " + getFailure
						+ " Set successes: " + setSuccess + "; set failures: " + setFailure,
						threadName);
	}

	private void runSets(String threadName) throws InterruptedException {
		ArrayList<Future<Boolean>> gotten = new ArrayList<Future<Boolean>>();
		for (int i = 0; i < 1000; i++) {
			gotten.add(mc.set(threadName + i, 0, threadName + i));
		}

		for (Future<Boolean> result : gotten) {
			try {
				if (result.get()) {
					setSuccess++;
				} else {
					setFailure++;
				}
			} catch (ExecutionException e) {
				// don't stop the thread, just log and move on
				Logger.getLogger(SleepingTester.class.getName()).log(Level.WARNING,
								"RuntimeException while checking sets in the future", e);
				setFailure++;
			} catch (Exception e) {
				Logger.getLogger(SleepingTester.class.getName()).log(Level.WARNING,
								"Unexpected Exception while checking gets in the future", e);
				getFailure++;
			}


		}
	}

	private void runGets(String threadName) throws InterruptedException,
					ExecutionException {
		// do some gets and check on the results
		ArrayList<Future<Object>> gotten = new ArrayList<Future<Object>>();
		for (int i = 0; i < 10000; i++) {
			try {
				gotten.add(mc.asyncGet(threadName + i));
			} catch (Exception e) {
				Logger.getLogger(SleepingTester.class.getName()).log(Level.WARNING,
								"RuntimeException while building get futures to check later",
								e);
			}
		}

		for (Future<Object> result : gotten) {
			try {
				result.get();
				getSuccess++;
			} catch (ExecutionException e) {
				// don't stop the thread, just log and move on
				Logger.getLogger(SleepingTester.class.getName()).log(Level.WARNING,
								"RuntimeException while checking gets in the future", e);
				getFailure++;
			} catch (Exception e) {
				Logger.getLogger(SleepingTester.class.getName()).log(Level.WARNING,
								"Unexpected Exception while checking gets in the future", e);
				getFailure++;
			}

		}

	}
}
