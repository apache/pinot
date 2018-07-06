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

import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

public class SimpleServerBlackList {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleServerBlackList.class);
    public static final String BLACK_LIST_PATH = "pinot-broker/src/main/resources/blacklist.config";

    public static List<Map<String, List<String>>> applyBlackList(List<Map<String, List<String>>> routingTables) {
        String blackListFile;
        List<String> blackList = new ArrayList<String>();
        //String pinotHome = System.getenv("PINOT_HOME");
        String pinotHome = "/home/sajavadi/pinot/";
        blackListFile = pinotHome + BLACK_LIST_PATH;
        try
        {
            InputStream inStream = new FileInputStream(blackListFile);
            Scanner scanner = new Scanner(inStream);
            while(scanner.hasNextLine()){
                blackList.add(scanner.nextLine());
            }
            scanner.close();
        }
        catch (Exception e)
        {
            LOGGER.error(e.getMessage());
            return routingTables;
        }

        for(int i=0;i<routingTables.size();i++)
        {
            Map<String, List<String>> routingTable = routingTables.get(i);
            if(!blackList.isEmpty())
            {
                String server = blackList.get(0);
                if(routingTable.containsKey(server) && routingTable.keySet().size()>1)
                {
                    List<String> serverSegs = routingTable.get(server);
                    //routingTable.remove(server);
                    //while(serverSegs != null && serverSegs.size()>1)
                    while(serverSegs != null && !serverSegs.isEmpty())
                    {
                        for (String key : routingTable.keySet()) {
                            //if(!key.equalsIgnoreCase(server) && serverSegs.size()>1)
                            if(!key.equalsIgnoreCase(server) && !serverSegs.isEmpty())
                            {
                                routingTable.get(key).add(serverSegs.get(0));
                                serverSegs.remove(0);
                            }
                        }
                    }

                    for (String key : routingTable.keySet()) {
                        for (Iterator<String> iter = routingTable.get(key).listIterator(); iter.hasNext(); ) {
                            String seg = iter.next();
                            if (seg.equalsIgnoreCase("mytable_16282_16312_10 %")) {
                                iter.remove();
                            }
                        }
                    }

                    List<String> oldSegs = new ArrayList <String>();
                    oldSegs.add("mytable_16282_16312_10 %");
                    routingTable.put(server,oldSegs);
                }


            }
        }


        return routingTables;
    }
}
