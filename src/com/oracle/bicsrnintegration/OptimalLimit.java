package com.oracle.bicsrnintegration;

public class OptimalLimit {

	private double mean;
	private long count;
	
	public OptimalLimit() {
		mean = 0.0;
		count = 0;
	}
	
	public void add(double number) {
		mean = ((mean * (double) count) + number) / ((double) count + 1);
		count++;
	}
	
	public double get() {
		return mean;
	}
	
}