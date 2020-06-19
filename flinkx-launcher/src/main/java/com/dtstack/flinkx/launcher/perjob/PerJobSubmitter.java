/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.launcher.perjob;

import com.dtstack.flinkx.launcher.ClassLoaderType;
import com.dtstack.flinkx.launcher.YarnConfLoader;
import com.dtstack.flinkx.options.Options;
import com.dtstack.flinkx.util.MapUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

/**
 * Date: 2019/09/11
 * Company: www.dtstack.com
 * @author tudou
 */
public class PerJobSubmitter {
    private static final Logger LOG = LoggerFactory.getLogger(PerJobSubmitter.class);

    /**
     * submit per-job task
     * @param options LauncherOptions
     * @return
     * @throws Exception
     */
    public static String submit(Options options, File jarFile, String[] programArgs) throws Exception{
        LOG.info("start to submit per-job task, LauncherOptions = {}", options.toString());

        if (StringUtils.isEmpty(options.getYarnconf())) {
            throw new RuntimeException("parameters of yarn is required");
        }

        YarnConfiguration yarnConf = StringUtils.isEmpty(options.getYarnconf()) ? new YarnConfiguration() : YarnConfLoader.getYarnConf(options.getYarnconf());
        Configuration flinkConfig = StringUtils.isEmpty(options.getFlinkconf()) ? new Configuration() : GlobalConfiguration.loadConfiguration(options.getFlinkconf());
        flinkConfig.setString("classloader.resolve-order", "child-first");

        Properties conProp = MapUtil.jsonStrToObject(options.getConfProp(), Properties.class);
        ClusterSpecification clusterSpecification = FlinkPerJobResourceUtil.createClusterSpecification(conProp);
        clusterSpecification.setCreateProgramDelay(true);
        clusterSpecification.setConfiguration(flinkConfig);
        clusterSpecification.setClasspaths(new ArrayList<>());
        clusterSpecification.setEntryPointClass("com.dtstack.flinkx.Main");
        clusterSpecification.setJarFile(jarFile);

        if (StringUtils.isNotEmpty(options.getS())) {
            clusterSpecification.setSpSetting(SavepointRestoreSettings.forPath(options.getS()));
        }

        clusterSpecification.setProgramArgs(programArgs);
        clusterSpecification.setCreateProgramDelay(true);
        clusterSpecification.setYarnConfiguration(yarnConf);

        clusterSpecification.setClassLoaderType(ClassLoaderType.PARENT_FIRST_CACHE);

        PerJobClusterClientBuilder perJobClusterClientBuilder = new PerJobClusterClientBuilder();
        perJobClusterClientBuilder.init(yarnConf, flinkConfig, conProp);

        AbstractYarnClusterDescriptor descriptor = perJobClusterClientBuilder.createPerJobClusterDescriptor(options);
        ClusterClient<ApplicationId> clusterClient = descriptor.deployJobCluster(clusterSpecification, new JobGraph(), false);
        String applicationId = clusterClient.getClusterId().toString();
        Collection<JobStatusMessage> list = clusterClient.listJobs().get();
        LOG.info("job size:" + list.size());
        JobID jobId = list.iterator().next().getJobId();
        LOG.info("deploy per_job with appId: {}, jobId: {}", applicationId, jobId.toString());
        String finalStatus = null;
        while (true) {
            Collection<JobStatusMessage> list1 = clusterClient.listJobs().get();
            JobStatus state = list1.iterator().next().getJobState();
            LOG.info("job state: " + state);
            if (state.isTerminalState()) {
                finalStatus = state.name();
                break;
            }
            Thread.sleep(10000);
        }
        Map<String, OptionalFailure<Object>> map = clusterClient.getAccumulators(jobId);
        LOG.info("numRead: " + map.get("numRead").get());
        LOG.info("numWrite: " + map.get("numWrite").get());
        LOG.info("nErrors: " + map.get("nErrors").get());
        if ("FINISHED".equals(finalStatus)) {
            LOG.info("TASK DONE");
        }
        if ("FAILED".equals(finalStatus)) {
            LOG.info("TASK FAILED");
        }
        clusterClient.shutDownCluster();
        clusterClient.shutdown();
        return applicationId;
    }
}