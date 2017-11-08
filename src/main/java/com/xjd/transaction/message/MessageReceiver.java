package com.xjd.transaction.message;

/**
 * @author elvis.xu
 * @since 2017-11-07 10:56
 */
public interface MessageReceiver {
	void onMessage(Message message);
}
