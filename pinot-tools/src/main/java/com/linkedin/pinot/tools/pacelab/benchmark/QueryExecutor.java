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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
    protected Properties config;
    protected PostQueryCommand postQueryCommand;
    protected int _testDuration;
    protected String _dataDir;
    List<ExecutorService> _threadPool;
    public static final String defaultPath = "pinot_benchmark/query_generator_config/";
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

    public void start() throws InterruptedException {
        loadConfig();
        int threadCnt = Integer.parseInt(config.getProperty("ThreadCount"));
        _threadPool = new ArrayList<>();

        QueryTask queryTask = getTask(config);
        queryTask.setPostQueryCommand(this.postQueryCommand);

        for(int i=0; i < threadCnt; i++)
        {
            _threadPool.add(Executors.newFixedThreadPool(1));
        }
        for(int i=0; i < threadCnt; i++)
        {
            _threadPool.get(i).execute(queryTask);
        }
        //threadPool.awaitTermination(_testDuration, TimeUnit.SECONDS);
        //threadPool.shutdownNow();
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
            System.out.println("FileNotFoundException, Path should be PINOT_HOME/pinot_benchmark/query_generator_config/");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("IOException");
            e.printStackTrace();
        }finally {
            lock.unlock();
        }
    }

    public String getPathOfConfigFile(){
        String prop = defaultPath+getConfigFile();
        //String prop = getConfigFile();
        String config;
        String propDir = System.getenv("PINOT_HOME");
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
    public void shutdownThreadPool()
    {
        int threadCnt = Integer.parseInt(config.getProperty("ThreadCount"));
        for(int i=0; i<threadCnt;i++)
        {
            _threadPool.get(i).shutdown();
        }
    }
}
