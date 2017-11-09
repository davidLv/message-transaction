package com.xjd.transaction.message;

/**
 * @author elvis.xu
 * @since 2017-11-07 10:56
 */
public interface MessageReceiver<T extends Message> {
	void onMessage(T message) throws Throwable;
}
