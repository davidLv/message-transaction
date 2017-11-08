package com.xjd.transaction.message.sender;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author elvis.xu
 * @since 2017-11-07 14:45
 */
public class Context extends ConcurrentHashMap {

	public static class OneContext extends Context {
		protected Context globalContext;

		public OneContext(Context globalContext) {
			this.globalContext = globalContext;
		}

		public Context getGlobalContext() {
			return globalContext;
		}
	}
}
