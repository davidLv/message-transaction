package com.xjd.transaction.message.sender.spi;

import com.xjd.transaction.message.Message;
import com.xjd.transaction.message.sender.Context;

/**
 * @author elvis.xu
 * @since 2017-11-07 11:18
 */
public interface SenderSpi<T extends Message> {
	void send(Context.OneContext context, T message) throws Throwable;
}
