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
package org.apache.pinot.tools;

import java.lang.reflect.Field;
import org.kohsuke.args4j.Option;


public class AbstractBaseCommand {
  public static final String DEFAULT_ZK_ADDRESS = "localhost:2181";
  public static final String DEFAULT_CLUSTER_NAME = "PinotCluster";

  public AbstractBaseCommand(boolean addShutdownHook) {
    if (addShutdownHook) {
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          cleanup();
        }
      });
    }
  }

  public AbstractBaseCommand() {

  }

  public String getName() {
    return "BaseCommand";
  }

  public void printUsage() {
    System.out.println("Usage: " + this.getName());

    for (Field f : this.getClass().getDeclaredFields()) {
      if (f.isAnnotationPresent(Option.class)) {
        Option option = f.getAnnotation(Option.class);

        System.out.println(String
            .format("\t%-25s %-30s: %s (required=%s)", option.name(), option.metaVar(), option.usage(),
                option.required()));
      }
    }
    printExamples();
  }

  public void printExamples() {

  }

  public void cleanup() {
  }
}
