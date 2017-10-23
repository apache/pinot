package com.linkedin.pinot.transport.metrics;

import com.linkedin.pinot.common.restlet.resources.ServerLoadMetric;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Gandharv on 10/6/2017.
 */

public class NettyServerWorkload {

    public static final long CAPTURE_WINDOW = 10000;
    private final Map<String, List<ServerLoadMetric>> avgLoadMap;

    public NettyServerWorkload(){
        avgLoadMap = new HashMap<>();
    }

    public void addWorkLoad(String tableName, ServerLoadMetric load){
        if(avgLoadMap.containsKey(tableName)){
            List<ServerLoadMetric> list = avgLoadMap.get(tableName);
            ServerLoadMetric l = list.get(list.size()-1);
            if(l._timestamp + CAPTURE_WINDOW >= load._timestamp){
                //if incoming load within last window -> update window
                updateLastWindow(tableName, load);
            }else{
                load._numRequests = 1;
                list.add(load);
            }
        }else{
            ArrayList<ServerLoadMetric> list = new ArrayList<>();
            load._numRequests = 1;
            list.add(load);
            avgLoadMap.put(tableName, list);
        }
    }

    private void updateLastWindow(String tableName, ServerLoadMetric load) {
        List<ServerLoadMetric> list = avgLoadMap.get(tableName);
        ServerLoadMetric lastLoad = list.get(list.size()-1);
        Double currAvgLatency = lastLoad._avglatency;
        Double CurrAvgSegments = lastLoad._avgSegments;
        long n = lastLoad._numRequests;
        lastLoad._avglatency = (currAvgLatency*n + load._avglatency)/(n+1);
        lastLoad._avgSegments = (CurrAvgSegments*n + load._avgSegments)/(n+1);
        lastLoad._numRequests = n+1;
        list.set(list.size()-1, lastLoad);
    }

    public List<ServerLoadMetric> getAvgLoad(String tablename){
        if(avgLoadMap.containsKey(tablename)){
            return avgLoadMap.get(tablename);
        }else{
            return null;
        }
    }
}


