/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.benchmark;

import org.apache.pinot.benchmark.common.PinotBenchServiceApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotBenchmarkServiceStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotBenchmarkServiceStarter.class);

  private PinotBenchServiceApplication _pinotBenchServiceApplication;

  public PinotBenchmarkServiceStarter() {
    _pinotBenchServiceApplication = new PinotBenchServiceApplication();
  }

  public void start() {


    LOGGER.info("Starting Pinot benchmark service.");
    _pinotBenchServiceApplication.start(9008);

  }

  public void stop() {
    LOGGER.info("Stopping Pinot benchmark service...");
    _pinotBenchServiceApplication.stop();
  }


  public static void main(String[] args) throws InterruptedException {
    PinotBenchmarkServiceStarter starter = new PinotBenchmarkServiceStarter();

    try {
      LOGGER.info("Start starter.");
      starter.start();
    } catch (Exception e) {
      LOGGER.error("Error starting!!!", e);
    }

    int i = 0;
    while (i++ < 600L) {
      Thread.sleep(1000L);
    }

    System.out.println("Stopping!");
    LOGGER.info("Stopping!!!");

    starter.stop();
  }

}
