package com.xjd.transaction.message.sender.spi;

import com.xjd.transaction.message.Message;

/**
 * <Pre>
 * User Send  -
 *            ->(sync) save(message)
 *            =>(asyncMode) send(contextThreadLocal, message)
 * Timer fire -
 *            ->(sync) read() until no message
 *                   for each message
 *                       try {
 *                           send(contextThreadLocal, message)
 *                       } catch (Throwable t) {
 *                           log(t)
 *                           continue;
 *                       }
 * </Pre>
 * @author elvis.xu
 * @since 2017-11-07 15:12
 */
public interface AsyncSenderSpi<T extends Message> extends SenderSpi<T> {

	void save(T message);

	Iterable<T> read(boolean newBatch);

}
