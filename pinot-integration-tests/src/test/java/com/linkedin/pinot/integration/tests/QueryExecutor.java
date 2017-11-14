package com.linkedin.pinot.integration.tests;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class QueryExecutor {
    Properties config;

    public static QueryExecutor getInstance(){
        return null;
    }

    public static List<QueryExecutor> getTableExecutors() {
        List<QueryExecutor> queryExecutors = new ArrayList<>();
        queryExecutors.add(JobQueryExecutor.getInstance());
        return queryExecutors;
    }

    public void start() throws InterruptedException {
        loadConfig();
        int threadCnt = Integer.parseInt(config.getProperty("threads"));
        ExecutorService threadPool = Executors.newFixedThreadPool(threadCnt);
        threadPool.execute(getTask(config));
        threadPool.awaitTermination(1, TimeUnit.MINUTES);
        threadPool.shutdownNow();
    }

    public void loadConfig() {
        String configFile = getConfigFile();
        config = new Properties();
        try {
            InputStream in = QueryExecutor.class.getClassLoader().getResourceAsStream(configFile);
            config.load(in);
        } catch (FileNotFoundException e) {
            System.out.println("FileNotFoundException");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("IOException");
            e.printStackTrace();
        }
    }

    public abstract String getConfigFile();

    public abstract QueryTask getTask(Properties config);
}
