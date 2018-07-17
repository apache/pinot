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
package com.linkedin.pinot.broker.routing.builder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

public class WorkerWeightDeployer {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerWeightDeployer.class);
    public static final String BLACK_LIST_PATH = "pinot-broker/src/main/resources/worker_weights.config";

    public static List<Map<String, List<String>>> applyWorkerWeights(List<Map<String, List<String>>> routingTables) {
        String workerWeightFile;

        List<String> configLine = new ArrayList<>();
        List<String> workerList = new ArrayList<String>();
        List<Float> weightList = new ArrayList<>();

        //String pinotHome = System.getenv("PINOT_HOME");
        String pinotHome = "/home/sajavadi/pinot/";
        workerWeightFile = pinotHome + BLACK_LIST_PATH;
        try
        {
            InputStream inStream = new FileInputStream(workerWeightFile);
            Scanner scanner = new Scanner(inStream);
            while(scanner.hasNextLine()){
                configLine.add(scanner.nextLine());
            }
            scanner.close();
            inStream.close();

        }
        catch (Exception e)
        {
            LOGGER.error(e.getMessage());
            return routingTables;
        }

        for (int i=0; i < configLine.size();  i++)
        {
            workerList.add(configLine.get(i).split(" ")[0]);
            weightList.add(Float.parseFloat(configLine.get(i).split(" ")[1]));
        }

        if(!workerList.isEmpty()) {
            String server = workerList.get(0);
            float weight = weightList.get(0);

            int numOfRTWithNoSeg =  BalancedRandomRoutingTableBuilder.getDefaultNumRoutingTables() - (int)(weight * BalancedRandomRoutingTableBuilder.getDefaultNumRoutingTables());
            numOfRTWithNoSeg = Math.min(numOfRTWithNoSeg, routingTables.size());

            for(int i=0;i<numOfRTWithNoSeg;i++)
            {
                Map<String, List<String>> routingTable = routingTables.get(i);

                if(routingTable.containsKey(server) && routingTable.keySet().size()>1)
                {
                    List<String> serverSegs = routingTable.get(server);
                    while(serverSegs != null && !serverSegs.isEmpty())
                    {
                        for (String key : routingTable.keySet()) {
                            if(!key.equalsIgnoreCase(server) && !serverSegs.isEmpty())
                            {
                                routingTable.get(key).add(serverSegs.get(0));
                                serverSegs.remove(0);
                            }
                        }
                    }
                }


            }


        }

        return routingTables;
    }
}
