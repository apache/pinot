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
import java.util.Map;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.utils.SampleQueries;


public class MultiStageResourceTrackerQuickStart extends SingleStageResourceTrackingQuickStart {
  @Override
  protected List<String> getQueries() {
    return List.of(SampleQueries.COUNT_BASEBALL_STATS, SampleQueries.BASEBALL_STATS_SELF_JOIN,
        SampleQueries.BASEBALL_JOIN_DIM_BASEBALL_TEAMS);
  }

  @Override
  protected Map<String, String> getQueryOptions() {
    return Collections.singletonMap("queryOptions",
        CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE + "=true");
  }

  @Override
  public List<String> types() {
    return Collections.singletonList("MULTI_STAGE_RESOURCE_TRACKING");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "MULTI_STAGE_RESOURCE_TRACKING"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
