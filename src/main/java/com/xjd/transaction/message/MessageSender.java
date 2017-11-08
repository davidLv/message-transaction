package com.xjd.transaction.message;

/**
 * @author elvis.xu
 * @since 2017-11-07 10:49
 */
public interface MessageSender<T extends Message> {
	void send(T message);
}
