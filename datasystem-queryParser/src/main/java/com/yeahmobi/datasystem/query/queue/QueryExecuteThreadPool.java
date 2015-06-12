package com.yeahmobi.datasystem.query.queue;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.yeahmobi.datasystem.query.meta.QueryEntry;


public class QueryExecuteThreadPool {
    private static Logger logger = Logger.getLogger(QueryExecuteThreadPool.class);
    
    public static void initCustomerPool() {
        Random random = new Random();
        /**
         * 产生一个 ExecutorService 对象，这个对象带有一个大小为 poolSize 的线程池， 若任 务数量大于
         * poolSize,任务会被放在一个 queue 里顺序执行。这里开启10个线程对象，放入线程池
         */
        ExecutorService service = Executors.newFixedThreadPool(2);
        int waitTime = 500;
        if (QueryQueue.queue.size() > 0) {
            int poolSize = 0;
            // 从线程池获得线程对象，并执行多线程查询操作
            for (int i = 1; i <= QueryQueue.queue.size() + 2; i++) {
                int time = random.nextInt(1000);
                waitTime += time;
                Runnable runner = new QueryExecuteThread(QueryQueue.queue.poll(), time);
                service.execute(runner);
                poolSize = i;
            }
            logger.info("QueryExecuteThreadPool Created, Size [" + poolSize + "].");
        }
        try {
            service.shutdown();// 关闭线程池，该周期内不再接受新的查询任务，对正在运行的任务不影响
            service.awaitTermination(waitTime, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("", e);
        }
    }
}
