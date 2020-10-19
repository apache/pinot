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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.InputStream;
import org.apache.pinot.compat.tests.BaseOp;
import org.apache.pinot.compat.tests.CompatTestOperation;

public class CompatibilityOpsRunner {
  private static final String ROOT_DIR = "compat-tests";

  private final String _configFileName;

  private CompatibilityOpsRunner(String configFileName) {
    _configFileName = configFileName;
  }

  private boolean runOps()
      throws IOException, JsonParseException, JsonMappingException {
    String filePath = ROOT_DIR + "/" + _configFileName;
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filePath);

    ObjectMapper om = new ObjectMapper(new YAMLFactory());
    CompatTestOperation operation = om.readValue(inputStream, CompatTestOperation.class);
    System.out.println("Running compat verifications from file:" + filePath + "(" + operation.getDescription() + ")");

    boolean passed = true;
    for (BaseOp op : operation.getOperations()) {
      if (!op.run()) {
        passed = false;
        System.out.println("Failure");
        break;
      }
    }
    return passed;
  }

  public static void main(String[] args) throws  Exception {
    if (args.length < 1 || args.length > 1) {
      throw new IllegalArgumentException("Need exactly one file name as argument");
    }

    CompatibilityOpsRunner runner = new CompatibilityOpsRunner(args[0]);
    int exitStatus = 1;
    if (runner.runOps()) {
      exitStatus = 0;
    }
    System.exit(exitStatus);
  }
}
