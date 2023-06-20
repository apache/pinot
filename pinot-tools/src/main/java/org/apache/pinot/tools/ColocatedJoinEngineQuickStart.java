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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.tools.admin.PinotAdministrator;


public class ColocatedJoinEngineQuickStart extends MultistageEngineQuickStart {
  private static final String QUICKSTART_IDENTIFIER = "COLOCATED_JOIN";
  private static final String[] COLOCATED_JOIN_DIRECTORIES = new String[]{
      "examples/batch/colocated/userAttributes", "examples/batch/colocated/userGroups"
  };

  @Override
  public List<String> types() {
    return Collections.singletonList(QUICKSTART_IDENTIFIER);
  }

  @Override
  public String[] getDefaultBatchTableDirectories() {
    return COLOCATED_JOIN_DIRECTORIES;
  }

  @Override
  protected int getNumQuickstartRunnerServers() {
    return 4;
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", QUICKSTART_IDENTIFIER));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
