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
import org.apache.pinot.tools.admin.command.QuickstartRunner;


/**
 * Quickstart with a table that has some null values in order to be able to play around with Pinot's null handling
 * related features.
 */
public class NullHandlingQuickstart extends Quickstart {

  private static final String[] NULL_HANDLING_TABLE_DIRS = new String[]{"examples/batch/clientSalaryNulls"};

  @Override
  public List<String> types() {
    return Collections.singletonList("NULL_HANDLING");
  }

  @Override
  public String[] getDefaultBatchTableDirectories() {
    return NULL_HANDLING_TABLE_DIRS;
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    printStatus(Quickstart.Color.YELLOW, "***** Null handling quickstart setup complete *****");

    Map<String, String> queryOptions = Collections.singletonMap("queryOptions",
        CommonConstants.Broker.Request.QueryOptionKey.ENABLE_NULL_HANDLING + "=true");

    printStatus(Quickstart.Color.YELLOW, "Total number of documents in the table");
    String query = "SELECT COUNT(*) FROM clientSalaryNulls";
    printStatus(Quickstart.Color.CYAN, "Query : " + query);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(query, queryOptions)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    printStatus(Quickstart.Color.YELLOW, "Total number of documents in the table with null salary values");
    query = "SELECT COUNT(*) FROM clientSalaryNulls WHERE salary IS NULL";
    printStatus(Quickstart.Color.CYAN, "Query : " + query);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(query, queryOptions)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    printStatus(Quickstart.Color.YELLOW, "Total number of documents in the table with non-null description");
    query = "SELECT COUNT(*) FROM clientSalaryNulls WHERE description IS NOT NULL";
    printStatus(Quickstart.Color.CYAN, "Query : " + query);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(query, queryOptions)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    printStatus(Quickstart.Color.YELLOW, "Minimum salary with null handling enabled");
    query = "SELECT MIN(salary) FROM clientSalaryNulls";
    printStatus(Quickstart.Color.CYAN, "Query : " + query);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(query, queryOptions)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    printStatus(Quickstart.Color.YELLOW, "Minimum salary without null handling enabled");
    query = "SELECT MIN(salary) FROM clientSalaryNulls";
    printStatus(Quickstart.Color.CYAN, "Query : " + query);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(query)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    printStatus(Quickstart.Color.YELLOW, "Count where salary is less than 80000");
    query = "SELECT COUNT(*) FROM clientSalaryNulls WHERE salary < 80000";
    printStatus(Quickstart.Color.CYAN, "Query : " + query);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(query, queryOptions)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    printStatus(Quickstart.Color.YELLOW, "Count where salary is less than 80000 (without null handling enabled)");
    query = "SELECT COUNT(*) FROM clientSalaryNulls WHERE salary < 80000";
    printStatus(Quickstart.Color.CYAN, "Query : " + query);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(query)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "NULL_HANDLING"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[0]));
  }
}
