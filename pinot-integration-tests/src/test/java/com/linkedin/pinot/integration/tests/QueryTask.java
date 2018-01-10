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
package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.tools.admin.command.PostQueryCommand;
import org.json.JSONObject;

import java.util.Properties;
import java.util.Random;

import static com.linkedin.pinot.tools.Quickstart.prettyPrintResponse;

public class QueryTask implements Runnable{
    private Properties config;
    private String[] queries;
    private Random rand = new Random();
    private PostQueryCommand postQueryCommand;

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
        while(!Thread.interrupted()){
            try {
                float[] likelihood = getLikelihoodArrayFromProps();
                float randomLikelihood = rand.nextFloat();

                for (int i = 0; i < likelihood.length; i++) {
                    if (randomLikelihood < likelihood[i]) {
                        generateAndRunQuery(i);
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private float[] getLikelihoodArrayFromProps() {
        String[] a = config.getProperty("likelihood").split(",");
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

    public void generateAndRunQuery(int queryId) throws Exception {

    }

    public PostQueryCommand getPostQueryCommand() {
        return this.postQueryCommand;
    }

    public void setPostQueryCommand(PostQueryCommand postQueryCommand) {
        this.postQueryCommand = postQueryCommand;
    }

}
