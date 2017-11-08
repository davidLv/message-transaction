package com.xjd.transaction.message;

/**
 * @author elvis.xu
 * @since 2017-11-07 15:32
 */
public class MessageException extends RuntimeException {
	public MessageException() {
	}

	public MessageException(String message) {
		super(message);
	}

	public MessageException(String message, Throwable cause) {
		super(message, cause);
	}

	public MessageException(Throwable cause) {
		super(cause);
	}

	public MessageException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public static class SpiException extends MessageException {
		public SpiException(Throwable cause) {
			super(cause);
		}
	}

	public static class TimeoutException extends MessageException {
		public TimeoutException(String message) {
			super(message);
		}
	}
}
