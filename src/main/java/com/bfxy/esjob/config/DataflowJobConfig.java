/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.bfxy.esjob.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.bfxy.esjob.task.SpringDataflowJob;
import com.dangdang.ddframe.job.api.dataflow.DataflowJob;
import com.dangdang.ddframe.job.config.JobCoreConfiguration;
import com.dangdang.ddframe.job.config.dataflow.DataflowJobConfiguration;
import com.dangdang.ddframe.job.event.JobEventConfiguration;
import com.dangdang.ddframe.job.lite.api.JobScheduler;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.spring.api.SpringJobScheduler;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperRegistryCenter;

/**
 * 流式处理，不是按照cron定时处理，而是不停的去处理
 */
@Configuration
public class DataflowJobConfig {
    
	@Autowired
    private ZookeeperRegistryCenter regCenter;
    
    @Autowired
    private JobEventConfiguration jobEventConfiguration;

    /**
     * 处理业务的地方
     * @return
     */
    @Bean
    public DataflowJob dataflowJob() {
        return new SpringDataflowJob();
    }
    
    @Bean(initMethod = "init")
    public JobScheduler dataflowJobScheduler(final DataflowJob dataflowJob, @Value("${dataflowJob.cron}") final String cron,
                                             @Value("${dataflowJob.shardingTotalCount}") final int shardingTotalCount,
                                             @Value("${dataflowJob.shardingItemParameters}") final String shardingItemParameters) {
       
    	SpringJobScheduler springJobScheduler = new SpringJobScheduler(dataflowJob, regCenter, getLiteJobConfiguration(dataflowJob.getClass(), cron,
                shardingTotalCount, shardingItemParameters), jobEventConfiguration);
//    	springJobScheduler.init();
    	return springJobScheduler;
    }
    
    private LiteJobConfiguration getLiteJobConfiguration(final Class<? extends DataflowJob> jobClass, final String cron, final int shardingTotalCount, final String shardingItemParameters) {
        return LiteJobConfiguration.newBuilder(
        		new DataflowJobConfiguration(JobCoreConfiguration.newBuilder(jobClass.getName(), cron, shardingTotalCount)
        		.shardingItemParameters(shardingItemParameters).build(), 
        		jobClass.getCanonicalName(),
                        /**
                         * 如果为true，不按照cron定时处理，而是按照流式不停处理
                         */
        		true))	//streamingProcess
                /**
                 * 当为true时，每次启动会从本地配置文件读取这些配置；
                 * 当为false时，每次启动会从配置中心拉去；
                 * 实际生产中，会用flase，直接在zookeeper控制台进行参数的修改，而不用修改本地配置后重启服务
                 */
        		.overwrite(true)
        		.build();
    }
}
