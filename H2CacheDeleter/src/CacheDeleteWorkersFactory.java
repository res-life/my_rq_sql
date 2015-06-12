

import java.util.concurrent.ThreadFactory;

/**
 * 用于设置线程池thread的名�?
 *
 */
public class CacheDeleteWorkersFactory implements ThreadFactory {

	private static int id = 0;

	@Override
	public Thread newThread(Runnable r) {

		return new Thread(r, "db-memory-cache-delete-workers-" + id++);
	}
}
