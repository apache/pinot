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
package com.linkedin.pinot.tools.pacelab.benchmark;

import com.linkedin.pinot.tools.admin.command.PostQueryCommand;
import com.linkedin.pinot.tools.admin.command.SegmentCreationCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.Max;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class MyProperties extends Properties{
    ReentrantLock lock;
    MyProperties(ReentrantLock plock){
        lock = plock;
    }
    public String getProperty(String key) {
        while(lock.isLocked()) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return super.getProperty(key);


    }

}
public abstract class QueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);
    protected Properties config;
    protected PostQueryCommand postQueryCommand;
    protected String _dataDir;
    protected String _recordFile;
    protected int _testDuration;
    protected int _slotDuration;
    protected AtomicBoolean[] thread_Status;

    public static final String QUERY_CONFIG_PATH = "pinot_benchmark/query_generator_config/";
    public static final String PINOT_TOOLS_RESOURCES = "pinot-tools/src/main/resources/";
    ReentrantLock lock = new ReentrantLock();

    public static QueryExecutor getInstance(){
        return null;
    }


    public static List<QueryExecutor> getTableExecutors() {
        List<QueryExecutor> queryExecutors = new ArrayList<>();
        queryExecutors.add(ProfileViewQueryExecutor.getInstance());
        //queryExecutors.add(JobApplyQueryExecutor.getInstance());
        //queryExecutors.add(AdClickQueryExecutor.getInstance());
        //queryExecutors.add(ArticleReadQueryExecutor.getInstance());
        //queryExecutors.add(CompanySearchQueryExecutor.getInstance());

        return queryExecutors;
    }

//    public void start() throws InterruptedException {
//        loadConfig();
//        //int threadCnt = Integer.parseInt(config.getProperty("ThreadCount"));
//        int threadCnt = Integer.parseInt(config.getProperty("QPS"));
//
//        List<ExecutorService> threadPool = new ArrayList<>();
//
//        QueryTask queryTask = getTask(config);
//        queryTask.setPostQueryCommand(this.postQueryCommand);
//
//        for(int i=0; i < threadCnt; i++)
//        {
//            //_threadPool.add(Executors.newFixedThreadPool(1));
//            threadPool.add(Executors.newSingleThreadScheduledExecutor());
//        }
//        for(int i=0; i < threadCnt; i++)
//        {
//            threadPool.get(i).execute(queryTask);
//        }
//
//        Thread.sleep(_testDuration*1000);
//        LOGGER.info("Test duration is completed! Ending threads then!");
//        for(int i=0; i<threadCnt;i++)
//        {
//            threadPool.get(i).shutdown();
//        }
//    }

    public void start() throws InterruptedException {
        loadConfig();
        List<Double> records = readFromTraces();

        int threadCnt = Integer.parseInt(config.getProperty("QPS"));
        double query_Factor = threadCnt/100;
        thread_Status = new AtomicBoolean[threadCnt+1];

        List<ExecutorService> threadPool = new ArrayList<>();

        QueryTaskDaemon queryTask;


        for(int i=0; i < threadCnt; i++)
        {
            //_threadPool.add(Executors.newFixedThreadPool(1));
            thread_Status[i]=new AtomicBoolean(false);
            threadPool.add(Executors.newSingleThreadScheduledExecutor());
        }
        for(int i=0; i < threadCnt; i++)
        {
            queryTask = (QueryTaskDaemon) getTask(config);
            queryTask.setPostQueryCommand(this.postQueryCommand);
            queryTask.setThread_status(thread_Status);
            queryTask.setThread_id(i);
            //System.out.println("INside thread");
            threadPool.get(i).execute(queryTask);
        }
        int prevThreadCount = 0;
        int currThreadCount = 0;

        boolean status;
        for(int i=0;i<records.size();i++){

            /*
            TODO: modify tread_status array according to the load
             */
            currThreadCount =  (int)(records.get(i)*query_Factor);
            if(prevThreadCount<=currThreadCount) status = true;
            else status =false;
            for(int j= Math.min(prevThreadCount,currThreadCount);j<=Math.max(prevThreadCount,currThreadCount);j++){
                thread_Status[j].set(status);
            }
            prevThreadCount = currThreadCount;
            Thread.sleep(_slotDuration*1000);
        }

        LOGGER.info("Test duration is completed! Ending threads then!");
        for(int i=0; i<threadCnt;i++)
        {
            threadPool.get(i).shutdown();
        }
    }
    //Will contain only one column
    private List<Double> readFromTraces(){
        List<Double> recordsList = new ArrayList<>();
        FileReader fileReader = null;
        BufferedReader bufferedReader = null;
        String line;
        try {
            File file = new File(_recordFile);
            fileReader = new FileReader(file);
            bufferedReader = new BufferedReader(fileReader);
            while ((line = bufferedReader.readLine()) != null) {
                recordsList.add(Double.parseDouble(line));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            closeStream(fileReader,bufferedReader);
        }
        return recordsList;
    }

    private void closeStream(Closeable... args) {
        for(Closeable file:args) {
            try {
                file.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private int calculateThreadCount(Double maxUtilization, double queryLoad) {
        //TODO from the load mapping find maximum thread count
        return (int)Math.ceil(maxUtilization/queryLoad);
    }


    public void loadConfig() {
        String configFile = getPathOfConfigFile();
        if(config==null){
            config = new MyProperties(lock);
        }

        try {
            InputStream in = new FileInputStream(configFile);
            lock.lock();
            config.clear();
            //InputStream in = QueryExecutor.class.getClassLoader().getResourceAsStream(configFile);
            config.load(in);
        } catch (FileNotFoundException e) {
            System.out.println("FileNotFoundException, Path should be PINOT_HOME/pinot-tools/src/main/resources/pinot_benchmark/query_generator_config/");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("IOException");
            e.printStackTrace();
        }finally {
            lock.unlock();
        }
    }

    public String getPathOfConfigFile(){
        String prop = PINOT_TOOLS_RESOURCES+QUERY_CONFIG_PATH+getConfigFile();
        //String prop = getConfigFile();
        String config;
        String propDir = System.getenv("PINOT_HOME");
        //String propDir = "/Users/robinmanhas/Desktop/StonyBrook/SecondSem/CSE523/pinot";
        if(propDir==null){
            //TODO We can load config from class loader also as default config to handle null pointer exception
            System.out.println("Environment variable is null. Check PINOT_HOME environment variable");
            return null;
        }
        if(propDir.endsWith("/"))
        {
            config = propDir + prop;
        }
        else
        {
            config = propDir + "/" + prop;
        }

        return config;
    }

    public void setPostQueryCommand(PostQueryCommand postQueryCommand) {
        this.postQueryCommand = postQueryCommand;
    }

    public abstract String getConfigFile();

    public abstract QueryTask getTask(Properties config);

    public void setTestDuration(int testDuration) {
        _testDuration = testDuration;
    }
    public void setDataDir(String dataDir)
    {
        _dataDir = dataDir;
    }
    public String getDataDir()
    {
        return _dataDir;
    }

    public void setRecordFile(String recordFile) {
        _recordFile = recordFile;
    }

    public void setSlotDuration(int slotDuration) {
        _slotDuration = slotDuration;
    }
}
