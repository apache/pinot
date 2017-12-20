/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix.core.sharding;

import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.IdealState;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LatencyBasedLoadMetric implements ServerLoadMetric {
    //private static final HttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
    //private static final Executor executor = Executors.newFixedThreadPool(1);
    private static final Logger logger = Logger.getLogger(LatencyBasedLoadMetric.class.getName());
    /**
     *  map to maintain cost of tables calculated by training logs.
      */
    private Map<String,Long> tableLatencyMap;
    /**
     *  map to maintain server specific costs.
     */
    private Map<String,Long> serverLatencyMap;
    private static final String COST_FILE = "latencyTrainedData/latency_load.csv";

    public LatencyBasedLoadMetric (){
        this.tableLatencyMap = new HashMap<>();
        this.serverLatencyMap = new HashMap<>();
        fillTableCosts();
    }

    private void fillTableCosts(){
        try{
            File trained_cost_file = new File("latency_load"+System.currentTimeMillis()+".log");

            ClassLoader classLoader = LatencyBasedLoadMetric.class.getClassLoader();
            URL resource = classLoader.getResource(COST_FILE);
            com.google.common.base.Preconditions.checkNotNull(resource);
            FileUtils.copyURLToFile(resource, trained_cost_file);
            logger.info("Latency Based Load metric reading trained cost file:"+trained_cost_file.getAbsolutePath());
            List<String> lines =  FileUtils.readLines(new File(trained_cost_file.getAbsolutePath()));
            //BufferedReader br = new BufferedReader(new FileReader(trained_cost_file));

                for (String line : lines) {
                    String[] info = line.split(",");
                    Long cost = 0l;
                    if (this.tableLatencyMap.containsKey(info[0])) {
                        cost = this.tableLatencyMap.get(info[0]);
                    }
                    cost = cost + (long)(double)Double.valueOf(info[2]);
                    this.tableLatencyMap.put(info[0], cost);
                    logger.info("Latency Based cost added for tableName : " + info[0] + " cost :" + cost);
                }

        }catch (Exception e){
            e.printStackTrace();
        }

        //tableLatencyMap.put("Job_OFFLINE",(long)(37.99312352282242));

    }

    public void addCostToServerMap(String instance, Long cost, PinotHelixResourceManager helixResourceManager){
        logger.info("Latency Based Load metric: Adding cost to server : "+instance + " cost:"+cost);
        this.serverLatencyMap.put(instance,cost);
        helixResourceManager.setServerLatencyMap(this.serverLatencyMap);
        for(String key : helixResourceManager.getServerLatencyMap().keySet()){
            logger.info("Post Server Latency Map Entry "+key + " cost:"+helixResourceManager.getServerLatencyMap().get(key));
        }

    }
    @Override
    public long computeInstanceMetric(PinotHelixResourceManager helixResourceManager, IdealState idealState, String instance, String tableName) {
 /*       ServerLatencyMetricReader serverlatencyMetricsReader =
                new ServerLatencyMetricReader(executor, connectionManager, helixResourceManager);
*/
        for(String key : helixResourceManager.getServerLatencyMap().keySet()){
            logger.info("Pre Server Latency Map Entry "+key + " cost:"+helixResourceManager.getServerLatencyMap().get(key));
        }
        this.serverLatencyMap = helixResourceManager.getServerLatencyMap();
        //ServerLoadMetrics serverLatencyInfo = serverlatencyMetricsReader.getServerLatencyMetrics(instance, tableName,true, 300);
        // Will Add logic to read from the model file.
        long tableLatencyCost = 0L;
        if(this.tableLatencyMap.containsKey(tableName))
             tableLatencyCost = this.tableLatencyMap.get(tableName);
        long serverLatencyCost = 0L;
        if(this.serverLatencyMap.containsKey(instance)){
            serverLatencyCost = this.serverLatencyMap.get(instance);
        }

        serverLatencyCost = serverLatencyCost + tableLatencyCost;
        //this.serverLatencyMap.put(instance,serverLatencyCost);
        return serverLatencyCost ;
    }

  /*public static void main(String args[]){
        System.out.println((long)(double)Double.valueOf(("37.4564545435")));
    }*/
}
