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
import io.swagger.models.auth.In;

import java.util.Properties;
import java.util.Random;

public class QueryTask implements Runnable{
    protected Properties config;
    protected String[] queries;
    protected Random rand = new Random(System.currentTimeMillis());
    protected PostQueryCommand postQueryCommand;
    protected String _dataDir;
    protected int _testDuration;
    //static int queryCount=0;
    protected int queryType = 0;
    protected Criteria criteria;

    public enum Color {
        RESET("\u001B[0m"),
        GREEN("\u001B[32m"),
        YELLOW("\u001B[33m"),
        CYAN("\u001B[36m");

        private String _code;

        Color(String code) {
            _code = code;
        }
    }

    @Override
    public void run() {
        long runStartMillisTime = System.currentTimeMillis();
        long currentTimeMillisTime = System.currentTimeMillis();

        float[] likelihood = getLikelihoodArrayFromProps();
        int QPS = Integer.parseInt(config.getProperty("QPS"));
        int ignoreQPS = Integer.parseInt(config.getProperty("ignoreQPs"));

        long secondsPassed = (currentTimeMillisTime-runStartMillisTime)/1000;
        //while(!Thread.interrupted()) {
        while(secondsPassed < _testDuration && !Thread.interrupted())
        {
            long timeBeforeSendingQuery = System.currentTimeMillis();

            try
            {

                float randomLikelihood = rand.nextFloat();
                for (int i = 0; i < likelihood.length; i++)
                {
                    if (randomLikelihood <= likelihood[i])
                    {
                        generateAndRunQuery(i, queryType);
                        break;
                    }
                }
                long timeAfterSendingQuery = System.currentTimeMillis();
                long timeDistance = timeAfterSendingQuery-timeBeforeSendingQuery;
                if (timeDistance < 1000)
                {
                    Thread.sleep(1000-timeDistance);
                }

            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

            currentTimeMillisTime = System.currentTimeMillis();
            secondsPassed = (currentTimeMillisTime-runStartMillisTime)/1000;
            //System.out.println(secondsPassed);
        }
    }

    private float[] getLikelihoodArrayFromProps() {
        String[] a = config.getProperty("LikelihoodVector").split(",");
        float[] likelihoodArray = new float[a.length];
        for(int i = 0;i < a.length;i++) {
            if (i == 0)
                likelihoodArray[i] = Float.parseFloat(a[i]);
            else
                likelihoodArray[i] = Float.parseFloat(a[i]) + likelihoodArray[i - 1];
        }
        return likelihoodArray;
    }

    private static void printStatus(QueryTask.Color color, String message) {
        System.out.println(color._code + message + QueryTask.Color.RESET._code);
    }

    public void runQuery(String query) throws Exception {
        printStatus(QueryTask.Color.CYAN, "Query:" + query);
        getPostQueryCommand().setQuery(query).run();
        //printStatus(QueryTask.Color.YELLOW, prettyPrintResponse(new JSONObject(getPostQueryCommand().setQuery(query).run())));
        //printStatus(QueryTask.Color.GREEN, "***************************************************");
    }

    public Properties getConfig() {
        return config;
    }

    public String[] getQueries() {
        return queries;
    }

    public void setConfig(Properties config) {
        this.config = config;
    }

    public void setQueries(String[] queries) {
        this.queries = queries;
    }

    public void generateAndRunQuery(int queryId, int queryType) throws Exception {

    }

    public PostQueryCommand getPostQueryCommand() {
        return this.postQueryCommand;
    }

    public void setPostQueryCommand(PostQueryCommand postQueryCommand) {
        this.postQueryCommand = postQueryCommand;
    }

    public void setDataDir(String dataDir)
    {
        _dataDir = dataDir;
    }

    public String getDataDir()
    {
        return _dataDir;
    }
    public void setTestDuration(int testDuration)
    {
        _testDuration = testDuration;
    }
    public int getQueryType() { 
	    return queryType;
    }
    public void setQueryType(int queryType) {
	    this.queryType = queryType;
    }
}
