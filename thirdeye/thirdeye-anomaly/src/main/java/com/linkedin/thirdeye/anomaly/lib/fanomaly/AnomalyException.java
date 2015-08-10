package com.linkedin.thirdeye.anomaly.lib.fanomaly;

public class AnomalyException extends Exception {
	public AnomalyException(String message) {
		super(message);
	}
	public AnomalyException(Throwable cause) {
		super(cause);
	}
	public AnomalyException(String message, Throwable cause) {
		super(message, cause);
	}

}
