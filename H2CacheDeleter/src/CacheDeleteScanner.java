

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * 启动�?��scanner 线程�?�?0分钟查库�?查询�?0分钟内快要过期的Cache<br>
 * 启动�?��schedule线程, 在指定时间点运行删除任务<br>
 * 这个删除任务是在workers线程池中进行�?保证不拖慢schedule线程
 * 
 */
public class CacheDeleteScanner {

	// 单例
	private static CacheDeleteScanner instance = new CacheDeleteScanner();

	// 五分钟扫描一�? 300�?
	private static int RATE = 300 * 1000;

	// 每次扫描�?选择�?30秒内过期的cache�?防止调度任务挂起�?耽误下一次扫描， 防止漏掉�?��cache
	private static int SCAN_SCOPE = 330 * 1000;

	// 系统启动十秒后开始扫�?
	private static int FIRST_START_DELAY = 10 * 1000;

	// 扫描线程
	ScheduledExecutorService scanner;

	// 调度线程
	ScheduledExecutorService scheduler;

	// 执行删除的线程池
	ExecutorService workers;

	/**
	 * 构�?函数
	 */
	private CacheDeleteScanner() {
		// �?0分钟运行�?��
		scanner = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {

				// 设置thread的名�?
				return new Thread(r, "db-memory-cache-delete-scnner");
			}
		});

		// 调度线程, 保证在指定时间点运行
		scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {

				// 设置thread的名�?
				return new Thread(r, "db-memory-cache-delete-scheduler");
			}
		});

		// 具体执行的线程池
		workers = Executors.newCachedThreadPool(new CacheDeleteWorkersFactory());
	}

	/**
	 * 单例模式
	 * 
	 * @return
	 */
	public static CacheDeleteScanner getInstance() {
		return instance;
	}

	/**
	 * 启动
	 */
	public void start() {
		scanner.scheduleAtFixedRate(CacheDeleteScheduler.newInstance(scheduler, workers, SCAN_SCOPE), FIRST_START_DELAY, RATE,
				TimeUnit.MILLISECONDS);
	}

	/**
	 * 停止
	 */
	public void stop() {
		scanner.shutdown();
		scheduler.shutdown();
		workers.shutdown();

		try {
			scanner.awaitTermination(15, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
		}

		try {
			scheduler.awaitTermination(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
		}

		try {
			workers.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
		}
	}

	/**
	 * 用于单元测试设置
	 * 
	 * @param firstStartDelay
	 */
	static void setFIRST_START_DELAY(int firstStartDelay) {
		FIRST_START_DELAY = firstStartDelay;
	}

	/**
	 * 用于单元测试设置
	 * 
	 * @param rate
	 */
	static void setRATE(int rate) {
		RATE = rate;
	}

	/**
	 * 用于单元测试设置
	 * 
	 * @param scanScope
	 */
	static void setSCAN_SCOPE(int scanScope) {
		SCAN_SCOPE = scanScope;
	}
}
