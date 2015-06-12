
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CacheDeleteScheduler implements Runnable {

	/**
	 * 负责在指定时间点将任务交给线程池处理, 因为是交给线程池异步处理<br>
	 * 返回会很�?br>
	 */
	private ScheduledExecutorService scheduler;

	/**
	 * 执行任务的线程池
	 */
	private ExecutorService workers;

	/**
	 * 查询时间区间, scanner是多长时间， 这个值就是多�?
	 */
	private int scanScope;

	/**
	 * 工厂方法�?创建scheduler
	 * 
	 * @param scheduler
	 * @param workers
	 * @param scanScope
	 * @return
	 */
	public static CacheDeleteScheduler newInstance(ScheduledExecutorService scheduler, ExecutorService workers,
			int scanScope) {
		return new CacheDeleteScheduler(scheduler, workers, scanScope);
	}

	/**
	 * 
	 * @param scheduler
	 *            调度线程�?
	 * @param workers
	 *            工作线程�?
	 * @param scanScope
	 *            单位�?
	 */
	private CacheDeleteScheduler(ScheduledExecutorService scheduler, ExecutorService workers, int scanScope) {
		this.scheduler = scheduler;
		this.workers = workers;
		this.scanScope = scanScope;
	}

	/**
	 * 周期执行调度
	 */
	@Override
	public void run() {

		System.out.println("begin schedule");
		try {
			// 1. 找到快要过期的entry
			long curr = System.currentTimeMillis();
			long target = curr + scanScope;

			List<List<Object>> rows = H2InMemoryDbUtil.selectTable("lp_meta", Arrays.asList("tablename", "timeouttime", "resulttable"),
					"cachestatus != 'NOT_CACHE' and timeouttime < " + target);

			// 2. 调度删除任务
			for (List<Object> entry : rows) {
				long at = Long.parseLong(entry.get(1).toString());
				curr = System.currentTimeMillis();
				long delay = at - curr;

				CacheRecord record = new CacheRecord();
				record.setResulttablename(entry.get(2).toString());
				record.setTablename(entry.get(0).toString());
				
				// 注意 delay 有可能有负�?�?会立即执�?
				scheduler.schedule(CacheDeleteRunnable.newInstance(workers, record), delay,
						TimeUnit.MILLISECONDS);
				
				System.out.println("scheduled one, delay is " + delay);
			}
		} catch (RejectedExecutionException e) {
			System.out.println("error: " + e);
		} catch (Exception e) {
			System.out.println("error: " + e);
		}
	}

}
