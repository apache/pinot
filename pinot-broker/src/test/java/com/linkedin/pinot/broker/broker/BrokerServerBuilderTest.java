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
package com.linkedin.pinot.broker.broker;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import org.apache.commons.configuration.PropertiesConfiguration;


public class BrokerServerBuilderTest {

  public static void main(String[] args) throws Exception {
    PropertiesConfiguration config =
        new PropertiesConfiguration(new File(BrokerServerBuilderTest.class.getClassLoader()
            .getResource("broker.properties").toURI()));
    final BrokerServerBuilder bld = new BrokerServerBuilder(config, null, null, null);
    bld.buildNetwork();
    bld.buildHTTP();
    bld.start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          bld.stop();
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    });

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      String command = br.readLine();
      if (command.equals("exit")) {
        bld.stop();
      }
    }

  }
}
