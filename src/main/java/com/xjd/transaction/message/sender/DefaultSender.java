package com.xjd.transaction.message.sender;

import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.extern.slf4j.Slf4j;

import com.xjd.transaction.message.Message;
import com.xjd.transaction.message.MessageException;
import com.xjd.transaction.message.MessageSender;
import com.xjd.transaction.message.sender.spi.AsyncSenderSpi;
import com.xjd.transaction.message.sender.spi.SenderSpi;
import com.xjd.utils.basic.AssertUtils;
import com.xjd.utils.basic.annotation.ThreadSafe;
import com.xjd.utils.basic.lock.ReserveLock;
import com.xjd.utils.basic.lock.impl.DefaultReserveLock;

/**
 * <pre>
 * 对于OrderedMessage的顺序保证以下:
 *
 * 1. 只保证在同一个DefaultSender中的发送顺序，在并发多个DefaultSender中的顺序不保证。
 *         -- 要想做到全局顺序，请使用一个DefaultSender，可以使用Leader模式。
 *         -- 在使用多个DefaultSender异步发送时，一定要实现SuggestedSenderSpi.lockMessage()方法，防止多个DefaultSender并发操作同一个消息
 *
 * 2. 只保证消息发送动作的顺序，先提交的一定先发送，但并不保证发送成功，如果前面的消息失败，后面的消息会继续发送，如果可重试，前面的消息就会在后面重试发送。
 *         -- 要想保证前面消息成功与否对后续消息的影响请在业务中实现。
 * </pre>
 * @author elvis.xu
 * @since 2017-11-07 11:15
 */
@Slf4j
@ThreadSafe
public class DefaultSender<T extends Message> implements MessageSender<T> {
	protected SenderSpi<T> senderSpi;
	protected Executor executor;

	protected Context globalContext = new Context();
	protected AtomicBoolean firing = new AtomicBoolean(false);

	public DefaultSender(SenderSpi<T> senderSpi) {
		AssertUtils.assertArgumentNonNull(senderSpi, "senderSpi cannot be null.");
		this.senderSpi = senderSpi;
	}

	public DefaultSender(AsyncSenderSpi<T> asyncSenderSpi, Executor executor) {
		AssertUtils.assertArgumentNonNull(asyncSenderSpi, "asyncSenderSpi cannot be null.");
		AssertUtils.assertArgumentNonNull(executor, "executor cannot be null.");
		this.senderSpi = asyncSenderSpi;
		this.executor = executor;
		log.info("using DefaultSender as 'async' mode, but no scheduler specified, please using an 'scheduler' to periodically schedule the fire() method for sending the failed messages.");
	}

	public DefaultSender(AsyncSenderSpi<T> asyncSenderSpi, ScheduledExecutorService scheduledExecutorService, long fixDelayInMillis) {
		AssertUtils.assertArgumentNonNull(asyncSenderSpi, "asyncSenderSpi cannot be null.");
		AssertUtils.assertArgumentNonNull(scheduledExecutorService, "scheduledExecutorService cannot be null.");
		AssertUtils.assertArgumentGreaterEqualThan(fixDelayInMillis, 1, "initialDelay must > 0");
		this.senderSpi = asyncSenderSpi;
		this.executor = scheduledExecutorService;
		scheduledExecutorService.scheduleWithFixedDelay(() -> {
			fire();
		}, fixDelayInMillis, fixDelayInMillis, TimeUnit.MILLISECONDS);
		fire(); // 初始化执行一次，主要为了保证有序消息的发送
	}

	@Override
	public void send(T message) {
		if (executor == null) {
			syncSend(message);
		} else {
			asyncSend(message);
		}
	}

	protected ReserveLock getReserveLockForKey(String orderKey) {
		if (orderKey == null) orderKey = "";

		ReserveLock reserveLock = (ReserveLock) globalContext.get(orderKey);
		if (reserveLock != null) return reserveLock;

		reserveLock = new DefaultReserveLock();
		ReserveLock existLock = (ReserveLock) globalContext.putIfAbsent(orderKey, reserveLock);
		if (existLock != null) return existLock;
		return reserveLock;
	}

	protected void invokeSpi(T message, boolean async) {
		try {
			senderSpi.send(new Context.OneContext(globalContext), message);
		} catch (Throwable t) {
			if (!async) {
				throw new MessageException.SpiException(t);
			} else {
				handleError(message, t);
			}
		}
	}

	public void syncSend(T message) {
		invokeSpi(message, false);
	}

	public void asyncSend(T message) {
		if (!(senderSpi instanceof AsyncSenderSpi)) {
			throw new MessageException("the senderSpi does not implement AsyncSenderSpi: " + senderSpi.getClass().getName());
		}
		AsyncSenderSpi<T> asyncSenderSpi = (AsyncSenderSpi) senderSpi;
		doAsyncSend(asyncSenderSpi, message, true);
	}

	public void fire() {
		if (!firing.compareAndSet(false, true)) return; // 有并发
		try {
			if (!(senderSpi instanceof AsyncSenderSpi)) {
				throw new MessageException("the senderSpi does not implement AsyncSenderSpi: " + senderSpi.getClass().getName());
			}
			AsyncSenderSpi<T> asyncSenderSpi = (AsyncSenderSpi) senderSpi;
			boolean newBatch = true;
			while (true) {
				Iterable<T> iterable = asyncSenderSpi.read(newBatch);
				newBatch = false;
				if (iterable == null) break;
				Iterator<T> it = iterable.iterator();
				if (!it.hasNext()) break;
				while (it.hasNext()) {
					T message = it.next();
					try {
						doAsyncSend(asyncSenderSpi, message, false);
					} catch (Throwable t) {
						handleError(message, t);
					}
				}
			}
		} finally {
			firing.set(false);
		}
	}

	protected void doAsyncSend(AsyncSenderSpi<T> asyncSenderSpi, T message, boolean save) {
		if (message instanceof Message.OrderedMessage && ((Message.OrderedMessage) message).isOrdered()) {
			Message.OrderedMessage orderedMessage = (Message.OrderedMessage) message;
			ReserveLock reserveLock = getReserveLockForKey(orderedMessage.getOrderKey());
			ReserveLock.Voucher voucher = null;
			if (save) {
				synchronized (reserveLock) { // 此处锁用于保证 插入数据库的顺序 和 执行顺序一致
					asyncSenderSpi.save(message);
					voucher = reserveLock.reserve();
				}
			} else {
				voucher = reserveLock.reserve();
			}
			ReserveLock.Voucher fv = voucher;
			executor.execute(() -> {
				try (ReserveLock.Voucher thisVoucher = fv) {
					if (orderedMessage.getOrderTimeoutInMillis() < 0) {
						thisVoucher.await();
					} else {
						if (!thisVoucher.await(orderedMessage.getOrderTimeoutInMillis(), TimeUnit.MILLISECONDS)) {
							throw new MessageException.TimeoutException("order timeout!");
						}
					}
					invokeSpi(message, true);
				} catch (Throwable t) {
					handleError(message, t);
				}
			});
		} else {
			if (save) {
				asyncSenderSpi.save(message);
			}
			executor.execute(() -> {
				invokeSpi(message, true);
			});
		}

	}

	public void handleError(Message message, Throwable t) {
		log.error("message send exception: key={}", message.getKey(), t);
	}
}
