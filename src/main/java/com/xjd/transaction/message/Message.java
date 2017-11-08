package com.xjd.transaction.message;

/**
 * @author elvis.xu
 * @since 2017-11-07 10:56
 */
public interface Message<T> {
	long getTimestamp();
	String getKey();
	T getValue();

	public static interface OrderedMessage<T> extends Message<T> {
		boolean isOrdered();
		String getOrderKey();
		long getOrderTimeoutInMillis();
	}
}
