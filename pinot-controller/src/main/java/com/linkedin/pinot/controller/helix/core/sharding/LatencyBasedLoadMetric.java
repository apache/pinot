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

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.core.query.utils.Pair;
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
    private static final Logger logger = Logger.getLogger(LatencyBasedLoadMetric.class.getName());
    /**
     *  map to maintain cost of tables calculated by training logs.
      */
    private Map<String,Pair<Float,Float>> _tableLatencyMap;
    /**
     *  map to maintain server specific costs.
     */
    private static final String COST_FILE = "latencyTrainedData/latency_load.csv";

    public LatencyBasedLoadMetric (){
        _tableLatencyMap = new HashMap<>();
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
            for(int i=1; i<lines.size();i++)
            {
                String[] costRecord = lines.get(i).split(",");
                _tableLatencyMap.put(costRecord[0], new Pair(Float.parseFloat(costRecord[1]),Float.parseFloat(costRecord[2])));
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public double computeInstanceMetric(PinotHelixResourceManager helixResourceManager, IdealState idealState, String instance, String tableName) {
        /*
        Map<String,Double> serverLoadMap = helixResourceManager.getServerLoadMap();
        if(serverLoadMap.containsKey(instance))
        {
            return serverLoadMap.get(instance);
        }
        else
        {
            return 0;
        }
        */
        return 0;
    }

    @Override
    public void updateServerLoadMetric(PinotHelixResourceManager helixResourceManager, String instance, Double currentLoadMetric, String tableName, SegmentMetadata segmentMetadata) {
        /*
        Pair<Float,Float> tableCostRecord = _tableLatencyMap.get(tableName);
        double newServerLoadMetric = currentLoadMetric + (segmentMetadata.getTotalDocs()/tableCostRecord.getFirst())*tableCostRecord.getSecond();
        helixResourceManager.updateServerLoadMap(instance,newServerLoadMetric);
        */
    }

    @Override
    public void resetServerLoadMetric(PinotHelixResourceManager helixResourceManager, String instance) {
        //helixResourceManager.updateServerLoadMap(instance,0D);
    }

  /*public static void main(String args[]){
        System.out.println((long)(double)Double.valueOf(("37.4564545435")));
    }*/
}
