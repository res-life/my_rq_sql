

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * 删除cache线程<br>
 * 调用者将其放在线程池中执行， 这样不会拖慢调用者线�?br>
 * 
 */
public class CacheDeleteRunnable implements Runnable {

	// 线程�?
	private ExecutorService workers;

	// 要删除的cache
	private CacheRecord cacheEntry;

	/**
	 * 工厂方法
	 * 
	 * @param workers
	 * @param cacheEntry
	 * @return
	 */
	public static CacheDeleteRunnable newInstance(ExecutorService workers, CacheRecord cacheEntry) {
		return new CacheDeleteRunnable(workers, cacheEntry);
	}

	/**
	 * 构�?函数
	 * 
	 * @param workers
	 * @param cacheEntry
	 */
	private CacheDeleteRunnable(ExecutorService workers, CacheRecord cacheEntry) {
		this.workers = workers;
		this.cacheEntry = cacheEntry;
	}

	@Override
	public void run() {

		// 在线程池里异步处理， 快�?返回�?不会拖慢调用者线�?
		try {
			workers.submit(new Runnable() {
				@Override
				public void run() {
					System.out.println("begin delete");
					try {
						String sql = String.format("delete from lp_meta where tablename ='%s'", cacheEntry.getTablename());
						H2InMemoryDbUtil.executeDbStatement(sql, "");
						System.out.println(sql);
					} catch (Exception e) {
						System.out.println("delete row error" + e);
					}

					try {
						String sql2 = "drop table if exists " + cacheEntry.getResulttablename();
						H2InMemoryDbUtil.executeDbStatement(sql2, "");
						System.out.println(sql2);
					} catch (Exception e) {
						System.out.println("drop table error" + e);
					}
				}
			});
		} catch (RejectedExecutionException e) {
			System.out.println("error: " + e);
		} catch (Exception e){
			System.out.println("error: " + e);
		}
	}
}
