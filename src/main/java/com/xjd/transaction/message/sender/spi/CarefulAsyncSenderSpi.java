package com.xjd.transaction.message.sender.spi;

import com.xjd.transaction.message.Message;
import com.xjd.transaction.message.sender.Context;

/**
 * @author elvis.xu
 * @since 2017-11-07 11:22
 */
public interface CarefulAsyncSenderSpi<T extends Message> extends AsyncSenderSpi<T> {

	@Override
	default void send(Context.OneContext context, T message) throws Throwable {
		if (beforeSend(context, message)) {
			Throwable t = null;
			try {
				doSend(context, message);
			} catch (Throwable t1) {
				t = t1;
			}
			afterSend(context, message, t);
		}
	}

	boolean beforeSend(Context.OneContext context, T message);

	void doSend(Context.OneContext context, T message) throws Throwable;

	void afterSend(Context.OneContext context, T message, Throwable t);

}
