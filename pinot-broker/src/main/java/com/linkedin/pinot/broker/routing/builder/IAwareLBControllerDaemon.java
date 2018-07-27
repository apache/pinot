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

public class IAwareLBControllerDaemon implements  Runnable{
    private static final Logger LOGGER = LoggerFactory.getLogger(IAwareLBControllerDaemon.class);
    public static final String Worker_Weight_List_Path = "pinot-broker/src/main/resources/worker_weights.config";
    public static final String Replica_Wise_Worker_Weigth_Path = "pinot-broker/src/main/resources/replica_wise_worker_weights.config";

    BalancedRandomRoutingTableBuilder _routingTableBuilder;
    private long _pastTimeStamp;
    private File _workerWeightListFile;


    public IAwareLBControllerDaemon(BalancedRandomRoutingTableBuilder randomRoutingTableBuilder)
    {
        _routingTableBuilder = randomRoutingTableBuilder;

        String pinotHome;

        if(System.getenv("PINOT_HOME")!= null)
        {
            pinotHome = System.getenv("PINOT_HOME");
        }
        else
        {
            pinotHome = "/home/sajavadi/pinot/";
        }

        //String workerWeightFilePath = pinotHome + Worker_Weight_List_Path;
         String workerWeightFilePath = pinotHome + Replica_Wise_Worker_Weigth_Path;

        _workerWeightListFile = new File(workerWeightFilePath);
        _pastTimeStamp = _workerWeightListFile.lastModified();
    }

    @Override
    public void run() {

        long newTimeStamp = _workerWeightListFile.lastModified();
        if(newTimeStamp != _pastTimeStamp)
        {
            _pastTimeStamp = newTimeStamp;
            _routingTableBuilder.computeRoutingTableFromLastUpdate();

        }
    }


    public  void setRoutingTableBuilder (BalancedRandomRoutingTableBuilder randomRoutingTableBuilder)
    {
        _routingTableBuilder = randomRoutingTableBuilder;
    }

}
