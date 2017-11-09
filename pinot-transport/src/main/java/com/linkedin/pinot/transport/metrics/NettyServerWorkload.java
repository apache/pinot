package com.linkedin.pinot.transport.metrics;

import com.linkedin.pinot.common.restlet.resources.ServerLatencyMetric;
import com.linkedin.pinot.common.restlet.resources.ServerLoadMetrics;
import org.apache.log4j.spi.ErrorCode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Gandharv on 10/6/2017.
 */

public class NettyServerWorkload {

    private static final int THREADS = (int) Math.round(Runtime.getRuntime().availableProcessors() * .75);
    private final Object writeMutex = new Object();
    protected int threads = THREADS;
    protected ExecutorService threadPool = Executors.newFixedThreadPool(THREADS);
    protected AsynchronousFileChannel log;
    public static final long CAPTURE_WINDOW = 1;
    public static final long FLUSH_WINDOW = 1;
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
            ServerLoadMetrics loadMetrics = new ServerLoadMetrics();
            loadMetrics.set_latencies(new ArrayList<ServerLatencyMetric>());
            load._numRequests = 1;
            loadMetrics.get_latencies().add(load);
            avgLoadMap.put(tableName, loadMetrics);
        }

        flushRecords(tableName);
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
            long size = list.size();
            String msg = "";
            StringBuilder builder = new StringBuilder(msg);
            while(size >= FLUSH_WINDOW){
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