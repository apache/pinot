/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.perf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.yaml.snakeyaml.Yaml;


/**
 * Simple code to run queries against a server
 * USAGE: QueryRunner confFile query numberOfTimesToRun
 */
public class QueryRunner {

  public static void multiThreadedQueryRunner(String confFile, String queryFile) throws Exception {
    PerfBenchmarkDriverConf conf = (PerfBenchmarkDriverConf) new Yaml().load(new FileInputStream(confFile));
    //since its only to run queries, we should ensure no services get started
    conf.setStartBroker(false);
    conf.setStartController(false);
    conf.setStartServer(false);
    conf.setStartZookeeper(false);
    conf.setUploadIndexes(false);
    conf.setRunQueries(true);
    conf.setConfigureResources(false);
    final PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);
    final List<String> queries = IOUtils.readLines(new FileInputStream(new File(queryFile)));
    final Random random = new Random();
    final int targetQps = 10;
    final int numClients = 3;
    final int sleepMillis = 1000 / (numClients * targetQps);
    ArrayList<Callable<Void>> tasks = new ArrayList<Callable<Void>>();

    for (int i = 0; i < numClients; i++) {
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          while (true) {
            String query = queries.get(random.nextInt(queries.size()));
            JSONObject response = driver.postQuery(query);
            Thread.sleep(sleepMillis);
          }
        }

      };
      tasks.add(callable);
    }
    List<Future<Void>> invokeAll = Executors.newFixedThreadPool(numClients).invokeAll(tasks);
    for (int i = 0; i < numClients; i++) {
      Future<Void> future = invokeAll.get(i);
      future.get();
    }

  }

  public static void runSingleQuery(String confFile, String query, int numRuns) throws Exception {
    PerfBenchmarkDriverConf conf = (PerfBenchmarkDriverConf) new Yaml().load(new FileInputStream(confFile));
    //since its only to run queries, we should ensure no services get started
    conf.setStartBroker(false);
    conf.setStartController(false);
    conf.setStartServer(false);
    conf.setStartZookeeper(false);
    conf.setUploadIndexes(false);
    conf.setRunQueries(true);
    conf.setConfigureResources(false);
    PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);

    for (int i = 0; i < numRuns; i++) {
      JSONObject response = driver.postQuery(query);
      System.out.println("Response:" + response);
      Thread.sleep(100);
    }
  }

  public static void main(String[] args) throws Exception {
    String confFile = args[0];
    String queryFile = args[1];
    multiThreadedQueryRunner(confFile, queryFile);

  }
}
