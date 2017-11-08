package com.xjd.transaction.message.sender.spi;

import com.xjd.transaction.message.Message;
import com.xjd.transaction.message.sender.Context;

/**
 * @author elvis.xu
 * @since 2017-11-07 16:36
 */
public interface SuggestedSenderSpi<T extends Message> extends CarefulAsyncSenderSpi<T> {
	@Override
	default boolean beforeSend(Context.OneContext context, T message) {
		if (!lockMessage(context, message)) return false;

		boolean flag = false;
		try {
			flag = messageSendable(context, message);
		} finally {
			if (!flag) {
				unlockMessage(context, message);
			}
		}
		return flag;
	}

	@Override
	default void afterSend(Context.OneContext context, T message, Throwable t) {
		try {
			doAfterSend(context, message, t);
		} finally {
			unlockMessage(context, message);
		}
	}

	boolean lockMessage(Context.OneContext context, T message);
	void unlockMessage(Context.OneContext context, T message);
	boolean messageSendable(Context.OneContext context, T message);
	void doAfterSend(Context.OneContext context, T message, Throwable t);
}
