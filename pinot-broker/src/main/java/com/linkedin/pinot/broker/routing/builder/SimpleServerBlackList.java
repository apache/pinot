package com.linkedin.pinot.broker.routing.builder;

import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

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
                if(routingTable.containsKey(server))
                {
                    List<String> serverSegs = routingTable.get(server);
                    //routingTable.remove(server);
                    while(serverSegs != null && serverSegs.size()>1)
                    {
                        for (String key : routingTable.keySet()) {
                            if(!key.equalsIgnoreCase(server) && serverSegs.size()>1)
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
