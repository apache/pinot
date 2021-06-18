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
package org.apache.pinot.compat.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CompatibilityOpsRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompatibilityOpsRunner.class);

  private String _parentDir;
  private final String _configFileName;
  private int _generationNumber;

  private CompatibilityOpsRunner(String configFileName, int generationNumber) {
    _configFileName = configFileName;
    _generationNumber = generationNumber;
  }

  private boolean runOps() throws Exception {
    Path path = Paths.get(_configFileName);
    _parentDir = path.getParent().toString();
    InputStream inputStream = Files.newInputStream(path);

    ObjectMapper om = new ObjectMapper(new YAMLFactory());
    CompatTestOperation operation = om.readValue(inputStream, CompatTestOperation.class);
    LOGGER.info("Running compat verifications from file:{} ({})", path.toString(), operation.getDescription());

    boolean passed = true;
    for (BaseOp op : operation.getOperations()) {
      op.setParentDir(_parentDir);
      if (!op.run(_generationNumber)) {
        passed = false;
        System.out.println("Failure");
        break;
      }
    }
    return passed;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      throw new IllegalArgumentException("Need exactly one file name and one generation_number as arguments");
    }
    String port;
    ClusterDescriptor.setControllerPort(System.getProperty("ControllerPort"));
    ClusterDescriptor.setBrokerQueryPort(System.getProperty("BrokerQueryPort"));
    ClusterDescriptor.setServerAdminPort(System.getProperty("ServerAdminPort"));

    CompatibilityOpsRunner runner = new CompatibilityOpsRunner(args[0], Integer.valueOf(args[1]));
    int exitStatus = 1;
    if (runner.runOps()) {
      exitStatus = 0;
    }
    System.exit(exitStatus);
  }
}
