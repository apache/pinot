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
package com.linkedin.pinot.transport.metrics;

import com.linkedin.pinot.common.restlet.resources.ServerLatencyMetric;
import com.linkedin.pinot.common.restlet.resources.ServerLoadMetrics;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class NettyServerWorkload {

    private static final int THREADS = (int) Math.round(Runtime.getRuntime().availableProcessors() * .75);
    private final Object writeMutex = new Object();
    protected int threads = THREADS;
    protected ExecutorService threadPool = Executors.newFixedThreadPool(THREADS);
    protected AsynchronousFileChannel log;
    public static final long CAPTURE_WINDOW = 1000;
    public static final long FLUSH_WINDOW = 1;
    private final Map<String, ServerLoadMetrics> avgLoadMap;

    public NettyServerWorkload(){
        avgLoadMap = new ConcurrentHashMap<>();
    }

    public void addWorkLoad(String tableName, ServerLatencyMetric load){
        if(avgLoadMap.containsKey(tableName)){
            //If already contains tableName get the load list for that table
            List<ServerLatencyMetric> list = avgLoadMap.get(tableName).get_latencies();
            //Get the last entry in the list
            ServerLatencyMetric lastLoad = list.get(list.size()-1);
            //System.out.println(lastElement.getTimestamp() + "-" + load.getTimestamp());
            if((load.getTimestamp() - lastLoad.getTimestamp()) <= CAPTURE_WINDOW){
                //if incoming load within last window then update window
                updateLastWindow(tableName, load);
            }else{
                //flush records to file, flushRecords will take care weather window has maxed out or not
                flushRecords(tableName);
                //else add new entry
                list.add(load);
            }
        }else {
            //if tableName doesn't exist till now
            ServerLoadMetrics loadMetrics = new ServerLoadMetrics();
            loadMetrics.set_latencies(new ArrayList<ServerLatencyMetric>());
            loadMetrics.get_latencies().add(load);
            avgLoadMap.put(tableName, loadMetrics);
        }
    }

    private void updateLastWindow(String tableName, ServerLatencyMetric load) {
        //Updating last entry in list with current load
        List<ServerLatencyMetric> list = avgLoadMap.get(tableName).get_latencies();
        ServerLatencyMetric lastLoad = list.get(list.size()-1);
        lastLoad.setLatency(lastLoad.getLatency() + load.getLatency());
        lastLoad.setSegmentSize(lastLoad.getSegmentSize() + load.getSegmentSize());
        lastLoad.setSegmentCount(lastLoad.getSegmentCount() + load.getSegmentCount());
        lastLoad.setNumRequests(lastLoad.getNumRequests() + 1);
        lastLoad.setDocuments(lastLoad.getDocuments() + load.getDocuments());
        //update the same index in list now
        //System.out.println(lastLoad.getTimestamp() + "-" + lastLoad.getNumRequests());
        list.set(list.size()-1, lastLoad);
    }

    public ServerLoadMetrics getAvgLoad(String tableName){
        if(avgLoadMap.containsKey(tableName)){
            return avgLoadMap.get(tableName);
        }else{
            return null;
        }
    }

    private void flushRecords(String tableName){
        String msg = getRecordsToWrite(tableName);
        if(null == msg || msg.length() <= 0){
            return;
        }

        String filePath = "target/workloadData/" + tableName + ".log";

        Path path = Paths.get(filePath);
        Path parentDir = path.getParent();

        if (null != parentDir && !Files.exists(parentDir)) {
            try {
                Files.createDirectories(parentDir);
                System.out.println("log file is created");
            } catch (IOException e) {
               e.printStackTrace();
            }
        }

        AtomicLong position = new AtomicLong(0);
        this.log = log(path, position);
        ByteBuffer buffer = ByteBuffer.allocateDirect(msg.length());
        buffer.put(msg.getBytes());
        buffer.flip();
        long pos = position.getAndAdd(msg.length());
        log.write(buffer, pos);
    }

    protected AsynchronousFileChannel log(Path path, AtomicLong position) {

        Set<OpenOption> openOptions = new HashSet<>();
        openOptions.add(StandardOpenOption.CREATE);
        openOptions.add(StandardOpenOption.WRITE);

        try {
            this.log = AsynchronousFileChannel.open(path, openOptions, threadPool);
            position.set(this.log.size());

        } catch (IOException e) {
           e.printStackTrace();
        }
        return log;
    }

    private String getRecordsToWrite(String tableName) {
        List<ServerLatencyMetric> list = avgLoadMap.get(tableName).get_latencies();
        if(list.size() >= FLUSH_WINDOW){
            long size = list.size()-1;
            String msg = "";
            StringBuilder builder = new StringBuilder(msg);
            while(size > 0){
                ServerLatencyMetric record = list.get(0);
                builder.append(record.toString());
                list.remove(0);
                size = size - 1;
            }

            if(list.size() == 0){
                avgLoadMap.remove(tableName);
            }
            return builder.toString();
        }else{
            return null;
        }
    }
}