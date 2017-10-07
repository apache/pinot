package com.linkedin.pinot.transport.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Gandharv on 10/6/2017.
 */
public class NettyServerWorkload {
    private final Map<String, List<Load>> loadMap;
    private final Map<String, Double> avgLatencyMap;
    private Map<String, Double> avgSegmentsMap;

    public NettyServerWorkload(){
        loadMap = new HashMap<>();
        avgLatencyMap = new HashMap<>();
        avgSegmentsMap = new HashMap<>();

        //read files here asynchronously
    }

    public void addWorkLoad(String tableName, Load load){
        if(loadMap.containsKey(tableName)){
            loadMap.get(tableName).add(load);
        }else{
            loadMap.put(tableName, new ArrayList<Load>());
            this.addWorkLoad(tableName, load);
        }

        updateAvgLatency(tableName, load.latency);
        updateAvgSegments(tableName, load.numSegments);

        //update file here asynchronously
    }

    private void updateAvgSegments(String tableName, int numSegments) {
        if(avgSegmentsMap.containsKey(tableName)){
            int loadCount = this.loadMap.get(tableName).size();
            double oldAvg = avgSegmentsMap.get(tableName);
            double newAvg = (oldAvg*(loadCount-1) + numSegments)/loadCount;
            avgSegmentsMap.put(tableName, newAvg);
        }else{
            avgSegmentsMap.put(tableName, (double) numSegments);
        }
    }

    private void updateAvgLatency(String tableName, double latency) {
        if(avgLatencyMap.containsKey(tableName)){
            int loadCount = this.loadMap.get(tableName).size();
            double oldAvg = avgLatencyMap.get(tableName);
            double newAvg = (oldAvg*(loadCount-1) + latency)/loadCount;
            avgLatencyMap.put(tableName, newAvg);
        }else{
            avgLatencyMap.put(tableName, latency);
        }
    }

    public Double getAvgLatency(String tablename){
        if(avgLatencyMap.containsKey(tablename)){
            return avgLatencyMap.get(tablename);
        }else{
            return 0.0;
        }
    }

    public Double getAvgSegments(String tablename){
        if(avgSegmentsMap.containsKey(tablename)){
            return avgSegmentsMap.get(tablename);
        }else{
            return 0.0;
        }
    }

    public Double getAvgLatency(String tablename, long startTime){
        if(loadMap.containsKey(tablename)) {
            List<Load> loadList = loadMap.get(tablename);
            if (loadList.isEmpty()) {
                return 0.0;
            } else {
                int entryCount = 0;
                double totalLatency = 0;
                for (Load l : loadList) {
                    if (l.timestamp >= startTime) {
                        totalLatency += l.latency;
                        entryCount++;
                    }
                }
                return totalLatency / entryCount;
            }
        }
        return 0.0;
    }

    public Double getAvgSegments(String tablename, long startTime){
        if(loadMap.containsKey(tablename)) {
            List<Load> loadList = loadMap.get(tablename);
            if (loadList.isEmpty()) {
                return 0.0;
            } else {
                int entryCount = 0;
                double totalSegments = 0;
                for (Load l : loadList) {
                    if (l.timestamp >= startTime) {
                        totalSegments += l.numSegments;
                        entryCount++;
                    }
                }
                return totalSegments / entryCount;
            }
        }
        return 0.0;
    }

    public static class Load{
        long timestamp;
        long latency;
        int numSegments;
    }
}


