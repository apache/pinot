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
import java.util.List;
import org.apache.pinot.tools.Quickstart.Color;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


public class RealtimeJsonIndexQuickStart extends RealtimeQuickStart {
  @Override
  public List<String> types() {
    return Arrays.asList("REALTIME_JSON_INDEX", "REALTIME-JSON-INDEX", "STREAM_JSON_INDEX", "STREAM-JSON-INDEX");
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    String q1 = "select json_extract_scalar(event_json, '$.event_name', 'STRING') from meetupRsvpJson where json_match"
        + "(group_json, '\"$.group_topics[*].topic_name\"=''topic_name0''') limit 10";
    printStatus(Color.YELLOW, "Events related to fitness");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));

    printStatus(Color.GREEN, "***************************************************");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "REALTIME-JSON-INDEX"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
