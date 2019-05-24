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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
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
	protected int _maxCPUInRecordFile;
	protected int _testDuration;
	protected int _slotDuration = 1;
	protected AtomicInteger[] threadQueryType;

	public static final String QUERY_CONFIG_PATH = "pinot_benchmark/query_generator_config/";
	public static final String PINOT_TOOLS_RESOURCES = "pinot-tools/src/main/resources/";
	protected final String CPU_LOAD_MAP_FILE = "cpu_load_map_file.csv";
	ReentrantLock lock = new ReentrantLock();
	private int threadCnt;


	private Criteria criteria;


	protected int _useCPUMap;
	protected Map<Integer,List<Integer>> mapCPULoadToThreadCounts;

	public static QueryExecutor getInstance(){
		return null;
	}


	public static List<QueryExecutor> getTableExecutors() {
		List<QueryExecutor> queryExecutors = new ArrayList<>();
		queryExecutors.add(ProfileViewQueryExecutor.getInstance());
		queryExecutors.add(JobApplyQueryExecutor.getInstance());
//queryExecutors.add(AdClickQueryExecutor.getInstance());
		queryExecutors.add(ArticleReadQueryExecutor.getInstance());
//queryExecutors.add(CompanySearchQueryExecutor.getInstance());

		return queryExecutors;
	}

	public void start() throws InterruptedException{
		loadConfig();

		if(_recordFile==null || _recordFile.isEmpty()) {
			System.out.println("Running Normal Code at "+System.currentTimeMillis());
			startQueryLoad();
		}else {
			System.out.println("Running Trace file Code at "+System.currentTimeMillis());
			startQueryTraceLoad();
		}
	}

	public void startQueryLoad() throws InterruptedException {

//int threadCnt = Integer.parseInt(config.getProperty("ThreadCount"));
		threadCnt = Integer.parseInt(config.getProperty(Constant.QPS));

		List<ExecutorService> threadPool = new ArrayList<>();

		QueryTask queryTask = getTask(config);
		queryTask.setPostQueryCommand(this.postQueryCommand);
		
		queryTask.setQueryType(Integer.parseInt(config.getProperty(Constant.QUERY_TYPE)));

		for(int i=0; i < threadCnt; i++)
		{
//_threadPool.add(Executors.newFixedThreadPool(1));
			threadPool.add(Executors.newSingleThreadScheduledExecutor());
		}
		for(int i=0; i < threadCnt; i++)
		{
			threadPool.get(i).execute(queryTask);
		}

		Thread.sleep(_testDuration*1000);
		LOGGER.info("Test duration is completed! Ending threads at "+System.currentTimeMillis());
		for(int i=0; i<threadCnt;i++)
		{
			threadPool.get(i).shutdown();
		}
	}



	public void startQueryTraceLoad() throws InterruptedException {
		List<Integer> records = readFromTraces();
		_testDuration = records.size()*_slotDuration;

		if(_useCPUMap != 0){
            threadCnt = loadCPUMap();
        }
        else{
            threadCnt = Integer.parseInt(config.getProperty(Constant.QPS));
        }

		threadQueryType = new AtomicInteger[threadCnt+1];
		List<ExecutorService> threadPool = new ArrayList<>();

		for(int i=0; i < threadCnt; i++) {
			threadQueryType[i]=new AtomicInteger(Constant.STOP);
			threadPool.add(Executors.newSingleThreadScheduledExecutor());
		}
		QueryTaskDaemon queryTask;
		for(int i=0; i < threadCnt; i++) {
			queryTask = (QueryTaskDaemon) getTask(config);
			queryTask.setPostQueryCommand(this.postQueryCommand);
			queryTask.setThreadQueryType(threadQueryType);
			queryTask.setThreadId(i);
			threadPool.get(i).execute(queryTask);
		}

		if(_useCPUMap != 0)
			executeBasedOnMap(records);
		else
			executeBasedOnQueryFactor(records,threadCnt/100, Integer.parseInt(config.getProperty(Constant.QUERY_TYPE)));

        LOGGER.info("Test duration is completed! Ending threads at "+System.currentTimeMillis());
		for(int i=0; i<threadCnt;i++) {
			threadQueryType[i].set(Constant.STOP);
			threadPool.get(i).shutdown();
		}
	}

	private void executeBasedOnQueryFactor(List<Integer> records, double queryFactor, int queryType) throws java.lang.InterruptedException{
		int prevThreadCount = 0;
		int currThreadCount;
		int currQueryType;
		for(int i=0;i<records.size();i++){
			currThreadCount =  (int)(records.get(i) * queryFactor);
			if(prevThreadCount > currThreadCount)
                currQueryType = Constant.STOP;
			else
                currQueryType = queryType;

            for(int j= Math.min(prevThreadCount,currThreadCount);j<Math.max(prevThreadCount,currThreadCount);j++){
				threadQueryType[j].set(currQueryType);
			}
			prevThreadCount = currThreadCount;
			Thread.sleep(_slotDuration*1000);
		}
	}

	private void executeBasedOnMap(List<Integer> records) throws java.lang.InterruptedException{
		List<Integer> threadCounts;
		for(int i=0;i<records.size();i++){
			threadCounts =  mapCPULoadToThreadCounts.get(records.get(i)); // convert
			int queryType=0;
			int curr=0;

            if(threadCounts == null){
			    continue;
            }

            for(int count: threadCounts){
                while(count>0 && curr <threadCnt){
                    threadQueryType[curr++].set(queryType);
                    count--;
                }
                queryType++;
			}

            while(curr < threadCnt) {
					threadQueryType[curr++].set(Constant.STOP);
			}

			Thread.sleep(_slotDuration*1000);
		}

	}
	private int loadCPUMap(){
		mapCPULoadToThreadCounts = new HashMap<>();
	 	FileReader fileReader = null;
	 	BufferedReader bufferedReader = null;
	 	String line;
	 	int maxThreadsInCpuLoadMap = 0;
		List<Integer> record;
	 	try {

	 	    File file = new File(getFullPath(PINOT_TOOLS_RESOURCES + CPU_LOAD_MAP_FILE));
			fileReader = new FileReader(file);
			bufferedReader = new BufferedReader(fileReader);
			while ((line = bufferedReader.readLine()) != null) {
				String[] values = line.split(",");
				if(values.length > 0){
				    int cpuLoad = Integer.parseInt(values[0]);
				    if(cpuLoad > _maxCPUInRecordFile)
				        break;

					record = new ArrayList<>();
					 for(int i =1; i<values.length; i++)
						 record.add(Integer.parseInt(values[i]));
					 mapCPULoadToThreadCounts.put(cpuLoad,record);

					 int curSum = 0;
					 for(Integer data : record) curSum += data;
					 if(curSum > maxThreadsInCpuLoadMap)
					     maxThreadsInCpuLoadMap = curSum;
				}
			}
		} catch (IOException e) {
			 e.printStackTrace();
		}finally {
			 closeStream(fileReader,bufferedReader);
		}

		return maxThreadsInCpuLoadMap;
	}

	//Will contain only one column
	private List<Integer> readFromTraces(){
		List<Integer> recordsList = new ArrayList<>();
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		String line;
		try {
			File file = new File(_recordFile);
			fileReader = new FileReader(file);
			bufferedReader = new BufferedReader(fileReader);
			while ((line = bufferedReader.readLine()) != null) {
			    int value = (int)Double.parseDouble(line);
			    if(value > _maxCPUInRecordFile)
			        _maxCPUInRecordFile = value;
			    recordsList.add(value);
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

	public void loadConfig() {
		String configFile = getFullPath(PINOT_TOOLS_RESOURCES+QUERY_CONFIG_PATH+getConfigFile());
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

	private String getFullPath(String filePath){
		String config;
		String propDir = System.getenv("PINOT_HOME");
		if(propDir==null){
//TODO We can load config from class loader also as default config to handle null pointer exception
			System.out.println("Environment variable is null. Check PINOT_HOME environment variable");
			return null;
		}
		if(propDir.endsWith("/"))
		{
			config = propDir + filePath;
		}
		else
		{
			config = propDir + "/" + filePath;
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

	public void setUseCPUMap(int _useCPUMap) {
		this._useCPUMap = _useCPUMap;
	}

	public Criteria getCriteria(String maxStartTime, String minStartTime) {
		if(criteria==null) {
			criteria = new Criteria(config,maxStartTime,minStartTime);
		}
		return criteria;
	}
}
