package com.linkedin.pinot.transport.metrics;

import com.linkedin.pinot.common.restlet.resources.ServerLatencyMetric;
import com.linkedin.pinot.common.restlet.resources.ServerLoadMetrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Gandharv on 10/6/2017.
 */

public class NettyServerWorkload {

    public static final long CAPTURE_WINDOW = 10000;
    private final Map<String, ServerLoadMetrics> avgLoadMap;

    public NettyServerWorkload(){
        avgLoadMap = new HashMap<>();
    }

    public void addWorkLoad(String tableName, ServerLatencyMetric load){
        if(avgLoadMap.containsKey(tableName)){
            List<ServerLatencyMetric> list = avgLoadMap.get(tableName).get_latencies();
            ServerLatencyMetric l = list.get(list.size()-1);
            if(l._timestamp + CAPTURE_WINDOW >= load._timestamp){
                //if incoming load within last window -> update window
                updateLastWindow(tableName, load);
            }else{
                load._numRequests = 1;
                list.add(load);
            }
        }else{
            ArrayList<ServerLatencyMetric> list = new ArrayList<>();
            load._numRequests = 1;
            list.add(load);
            ServerLoadMetrics loadMetrics = new ServerLoadMetrics();
            loadMetrics.set_latencies(list);
            avgLoadMap.put(tableName, loadMetrics);
        }
    }

    private void updateLastWindow(String tableName, ServerLatencyMetric load) {
        List<ServerLatencyMetric> list = avgLoadMap.get(tableName).get_latencies();
        ServerLatencyMetric lastLoad = list.get(list.size()-1);
        Double currAvgLatency = lastLoad._avglatency;
        Double CurrAvgSegments = lastLoad._avgSegments;
        long n = lastLoad._numRequests;
        lastLoad._avglatency = (currAvgLatency*n + load._avglatency)/(n+1);
        lastLoad._avgSegments = (CurrAvgSegments*n + load._avgSegments)/(n+1);
        lastLoad._numRequests = n+1;
        list.set(list.size()-1, lastLoad);
    }

    public ServerLoadMetrics getAvgLoad(String tablename){
        if(avgLoadMap.containsKey(tablename)){
            return avgLoadMap.get(tablename);
        }else{
            return null;
        }
    }
}


