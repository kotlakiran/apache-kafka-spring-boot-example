package com.kiran.springboot.kafka.consumer.executor;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


@Component
public class KafkaTestExecutorService extends ThreadPoolExecutor {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTestExecutorService.class);

	//@Autowired
	//private ThreadPoolMonitorService threadPoolMonitorService;

	@Value("${monitor.interval.seconds:60}")
	private long monitorInterval;

	private Thread monitorThread;

	@Inject
	public KafkaTestExecutorService(
			@Value("${executorService.corePoolSize:20}") int corePoolSize,
			@Value("${executorService.maximumPoolSize:30}") int maximumPoolSize,
			@Value("${executorService.keepAliveTimeMS:30000}") long keepAliveTimeMS,
			@Value("${executorService.workQueueSize:40000}") int workQueueSize) {
		super(corePoolSize, maximumPoolSize, keepAliveTimeMS, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue(workQueueSize), new KafkaTestExecutorRejectedExecutionHandler());
	}

	@Override
	protected void beforeExecute(Thread t, Runnable r) {
		LOGGER.debug("Task:{} starting in thread{}", r, t);
		super.beforeExecute(t, r);
	}

	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		super.afterExecute(r, t);
		if (t != null) {
			LOGGER.error("task:{} has exception:{}", r, t);
			LOGGER.error("Perform exception handler logic");
		}
		LOGGER.debug("task:{} completed", r);
	}

	@PostConstruct
	public void init() throws Exception {
		int results = prestartAllCoreThreads();
		LOGGER.info(String.format("Init method to start %s number of threads : ", results));
		startThreadPoolMonitoringService();
	}

	@PreDestroy
	public void cleanUp() throws Exception {
		final List<Runnable> rejected = shutdownNow();
		LOGGER.info("Rejected tasks: {}", rejected.size());
		LOGGER.info("executor service shutting down");
	}


	private void startThreadPoolMonitoringService() {
		LOGGER.info("Starting Thread Pool Monitoring Service with monitor interval: " + monitorInterval + " seconds");
		//threadPoolMonitorService.setExecutor(this);
		//threadPoolMonitorService.setMonitoringPeriod(monitorInterval);
		//monitorThread = new Thread(threadPoolMonitorService);
		//monitorThread.start();
	}
}

class KafkaTestExecutorRejectedExecutionHandler implements RejectedExecutionHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTestExecutorRejectedExecutionHandler.class);

	@Override
	public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
		LOGGER.error("RejectedExecutionHandler policy : Task:{} is rejected : ", r);
		String errorMessage = String.format("RejectedExecutionHandler policy : Task:%s is rejected : ", r);
		LOGGER.error(errorMessage);
		//throw error
	}
}
