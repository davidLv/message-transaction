package com.xjd.transaction.message.receiver.spi;

import com.xjd.transaction.message.Message;
import com.xjd.transaction.message.MessageReceiver;

/**
 * @author elvis.xu
 * @since 2017-11-08 17:56
 */
public interface ReceiverSpi<T extends Message> {

	MessageReceiver<T> getReceiver();

	default void process(T message) {
		int state = beforeProcess(message);
		if (state < 0) return;
		if (state == 0) {
			Throwable t = null;
			try {
				doProcess(message);
			} catch (Throwable t1) {
				t = t1;
			}
			afterProcess(message, t);
		} else {
			try {
				finishMessage(message);
			} finally {
				unlockMessage(message);
			}
		}
	}

	default int beforeProcess(T message) {
		if (!lockMessage(message)) return -1;

		int state = -1;
		try {
			state = messageProcessable(message);
		} finally {
			if (state == -1) {
				unlockMessage(message);
			}
		}
		return state;
	}

	default void doProcess(T message) throws Throwable {
		getReceiver().onMessage(message);
	}

	default void afterProcess(T message, Throwable t) {
		try {
			if (doAfterProcess(message, t)) {
				finishMessage(message);
			}
		} finally {
			unlockMessage(message);
		}
	}

	boolean lockMessage(T message);
	void unlockMessage(T message);
	int messageProcessable(T message);
	boolean doAfterProcess(T message, Throwable t);
	void finishMessage(T message);
}
