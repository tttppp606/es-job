package com.bfxy.esjob.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.bfxy.esjob.listener.SimpleJobListener;
import com.bfxy.esjob.task.MySimpleJob;
import com.dangdang.ddframe.job.api.simple.SimpleJob;
import com.dangdang.ddframe.job.config.JobCoreConfiguration;
import com.dangdang.ddframe.job.config.simple.SimpleJobConfiguration;
import com.dangdang.ddframe.job.event.JobEventConfiguration;
import com.dangdang.ddframe.job.lite.api.JobScheduler;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.spring.api.SpringJobScheduler;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperRegistryCenter;

//@Configuration
public class MySimpleJobConfig {

	@Autowired
	private ZookeeperRegistryCenter registryCenter/* @bean注册的，名字与方法名字一样 */;

	//创建作业事件监听器
	@Autowired
	private JobEventConfiguration jobEventConfiguration;
	
	/**
	 * 	具体真正的定时任务执行逻辑
	 * @return
	 */
	@Bean
	public SimpleJob simpleJob() {
		return new MySimpleJob();
	}
	
	/**
	 * JobScheduler是esjob的核心调度，构造它需要的信息：
	 * SimpleJob：自己的定时任务业务逻辑
	 * registryCenter：zookeeper的注册配置
	 * esjob定时任务的配置：
	 *      cron：定时任务时间
	 *      shardingTotalCount：分布式下分多少片
	 *      shardingItemParameters：如何划分分片
	 *      jobParameter：？？
	 *      failover：是否开启故障转移
	 *      monitorExecution：是否执行监控
	 *      monitorPort：监控端口
	 *      maxTimeDiffSeconds：是否忽略误差时间，-1是忽略
	 *      jobShardingStrategyClass：分片分配策略
	 * jobEventConfiguration：数据库配置，用于日志记录落库
	 * SimpleJobListener：可以在定时任务执行前后做一些操作
	 * @param simpleJob
	 * @return
	 */
	@Bean(initMethod = "init")/*一般这种方法都是必要的前置初始化*/
	public JobScheduler simpleJobScheduler(final SimpleJob simpleJob,
			@Value("${simpleJob.cron}") final String cron,
			@Value("${simpleJob.shardingTotalCount}") final int shardingTotalCount,
			@Value("${simpleJob.shardingItemParameters}") final String shardingItemParameters,
			@Value("${simpleJob.jobParameter}") final String jobParameter,
			@Value("${simpleJob.failover}") final boolean failover,
			@Value("${simpleJob.monitorExecution}") final boolean monitorExecution,
			@Value("${simpleJob.monitorPort}") final int monitorPort,
			@Value("${simpleJob.maxTimeDiffSeconds}") final int maxTimeDiffSeconds,
			@Value("${simpleJob.jobShardingStrategyClass}") final String jobShardingStrategyClass) {
		
		return new SpringJobScheduler(simpleJob,
				registryCenter,
				getLiteJobConfiguration(simpleJob.getClass(),
						cron,
						shardingTotalCount,
						shardingItemParameters,
						jobParameter,
						failover,
						monitorExecution,
						monitorPort,
						maxTimeDiffSeconds,
						jobShardingStrategyClass),
				jobEventConfiguration,
				new SimpleJobListener());
		
	}


	/**
	 * JobCoreConfiguration---->SimpleJobConfiguration----->liteJobConfiguration(最终获取的配置类）
	 * @return
	 */
	private LiteJobConfiguration getLiteJobConfiguration(Class<? extends SimpleJob> jobClass, String cron,
			int shardingTotalCount, String shardingItemParameters, String jobParameter, boolean failover,
			boolean monitorExecution, int monitorPort, int maxTimeDiffSeconds, String jobShardingStrategyClass) {

		JobCoreConfiguration jobCoreConfiguration = JobCoreConfiguration
				.newBuilder(jobClass.getName(), cron, shardingTotalCount)
				.misfire(true)
				.failover(failover)
				.jobParameter(jobParameter)
				.shardingItemParameters(shardingItemParameters)
				.build();
		
		SimpleJobConfiguration simpleJobConfiguration = new SimpleJobConfiguration(jobCoreConfiguration, jobClass.getCanonicalName());
		
		LiteJobConfiguration liteJobConfiguration = LiteJobConfiguration.newBuilder(simpleJobConfiguration)
				.jobShardingStrategyClass(jobShardingStrategyClass)
				.monitorExecution(monitorExecution)
				.monitorPort(monitorPort)
				.maxTimeDiffSeconds(maxTimeDiffSeconds)
				/**
				 * 当为true时，每次启动会从本地配置文件读取这些配置；
				 * 当为false时，每次启动会从配置中心拉去；
				 * 实际生产中，会用flase，直接在zookeeper控制台进行参数的修改，而不用修改本地配置后重启服务
				 */
				.overwrite(false)
				.build();
		
		return liteJobConfiguration;
	}
	
	
	
	
	
	
}
