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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IAwareLBControllerDaemon implements  Runnable{
    private static final Logger LOGGER = LoggerFactory.getLogger(IAwareLBControllerDaemon.class);
    public static final String Worker_Weight_List_Path = "pinot-broker/src/main/resources/worker_weights.config";
    BalancedRandomRoutingTableBuilder _routingTableBuilder;
    private long _pastTimeStamp;
    private File _workerWeightListFile;

    private volatile List<Map<String, List<String>>> _routingTables;


    public IAwareLBControllerDaemon(BalancedRandomRoutingTableBuilder randomRoutingTableBuilder)
    {
        _routingTableBuilder = randomRoutingTableBuilder;
        //String pinotHome = System.getenv("PINOT_HOME");
        String pinotHome = "/home/sajavadi/pinot/";
        String workerWeightFilePath = pinotHome + Worker_Weight_List_Path;
        _workerWeightListFile = new File(workerWeightFilePath);
        _pastTimeStamp = _workerWeightListFile.lastModified();

        _routingTables = new ArrayList<>();
    }

    @Override
    public void run() {

        long newTimeStamp = _workerWeightListFile.lastModified();
        if(newTimeStamp != _pastTimeStamp)
        {
            _pastTimeStamp = newTimeStamp;
            //routingTableBuilder.computeRoutingTableFromExternalView(tableName,externalView,instanceConfigs);
            //routingTableBuilder.computeRoutingTableFromExternalView(routingTableBuilder.getTableName(),routingTableBuilder.getExternalView(),routingTableBuilder.getInstanceConfigs());

            /*
            List<Map<String, List<String>>> routingTables;
            routingTables = WorkerWeightDeployer.applyWorkerWeights(_routingTables);
            _routingTableBuilder.setRoutingTables(routingTables);
            */
            _routingTableBuilder.computeRoutingTableFromLastUpdate();

        }
    }

    public void setRoutingTables(List<Map<String, List<String>>> routingTables)
    {
        _routingTables.clear();

        for(int i=0;i<routingTables.size();i++)
        {
            Map<String, List<String>> routingTable = routingTables.get(i);
            _routingTables.add(routingTable);
        }
    }
    public  void setRoutingTableBuilder (BalancedRandomRoutingTableBuilder randomRoutingTableBuilder)
    {
        _routingTableBuilder = randomRoutingTableBuilder;
    }
    /*
    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }
    public void setExternalView (ExternalView externalView)
    {
        this.externalView = externalView;
    }
    public void setInstanceConfigs (List<InstanceConfig> instanceConfigs)
    {
        this.instanceConfigs = instanceConfigs;
    }*/
}
