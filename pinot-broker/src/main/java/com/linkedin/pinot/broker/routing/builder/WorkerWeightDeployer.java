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
    public static final String Worker_Weight_Path = "pinot-broker/src/main/resources/worker_weights.config";
    public static final String Replica_Wise_Worker_Weight_Path = "pinot-broker/src/main/resources/replica_wise_worker_weights.config";


    public static List<Map<String, List<String>>> applyWorkerWeights(List<Map<String, List<String>>> routingTables, Map<String, List<String>> lastSegment2ServerMap) {
        String workerWeightFile;

        List<String> configLine = new ArrayList<>();

        Map<String, List<Float>> workerList = new HashMap<String, List<Float>>();

        String pinotHome;

        if(System.getenv("PINOT_HOME")!= null)
        {
            pinotHome = System.getenv("PINOT_HOME");
        }
        else
        {
            pinotHome = "/home/sajavadi/pinot/";
        }

        workerWeightFile = pinotHome + Replica_Wise_Worker_Weight_Path;
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
            String tokens[] = configLine.get(i).split(" ");
            String server = tokens[0];
            List<Float> weights = new ArrayList <>();
            for(int j=1; j<4;j++)
            {
                weights.add(Float.parseFloat(tokens[j]));
            }
            workerList.put(server,weights);
        }

        for (Map.Entry<String, List<Float>> entry : workerList.entrySet())
        {
            String server = entry.getKey();
            List<Float> weights  = entry.getValue();

            for(int i=0;i<routingTables.size();i++)
            {
                Map<String, List<String>> routingTable = routingTables.get(i);

                if(routingTable.containsKey(server) && routingTable.keySet().size()>1)
                {
                    List<String> serverSegs = routingTable.get(server);
                    for (ListIterator<String> iter = serverSegs.listIterator(); iter.hasNext(); ) {
                        String seg = iter.next();
                        List<String> segServers = lastSegment2ServerMap.get(seg);
                        String selectedServer = selectRandomServerBasedOnWeights(server,segServers,weights);
                        if(!selectedServer.equalsIgnoreCase(server))
                        {
                            routingTable.get(selectedServer).add(seg);
                            iter.remove();
                        }
                    }
                }
            }


        }

        return routingTables;
    }
    private  static String selectRandomServerBasedOnWeights(String targetServer, List<String> segServers, List<Float> weights)
    {

        List<String> serverList = new ArrayList <>();
        serverList.add(targetServer);
        for(String server :segServers)
        {
            if(!server.equalsIgnoreCase(targetServer))
            {
                serverList.add(server);
            }
        }
        if(segServers.size()==1)
        {
            return serverList.get(0);
        }
        else if (segServers.size()==2)
        {
            double rand = Math.random();
            if(rand<weights.get(0))
            {
                return serverList.get(0);
            }
            else
            {
                return serverList.get(1);
            }
        }
        else if (segServers.size()==3)
        {
            double rand = Math.random();
            if(rand<weights.get(0))
            {
                return serverList.get(0);
            }
            else if(rand < weights.get(0) + weights.get(1))
            {
                return serverList.get(1);
            }
            else
            {
                return serverList.get(2);
            }
        }

        return targetServer;
    }
    public static List<Map<String, List<String>>> applyWorkerWeights(List<Map<String, List<String>>> routingTables) {
        String workerWeightFile;

        List<String> configLine = new ArrayList<>();
        List<String> workerList = new ArrayList<String>();
        List<Float> weightList = new ArrayList<>();

        String pinotHome = System.getenv("PINOT_HOME");

        if(pinotHome.isEmpty())
        {
            pinotHome = "/home/sajavadi/pinot/";
        }

        workerWeightFile = pinotHome + Worker_Weight_Path;
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
