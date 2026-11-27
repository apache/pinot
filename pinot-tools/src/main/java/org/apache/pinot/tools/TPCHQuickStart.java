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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


/// Quickstart with the TPC-H tables at scale factor 0.01. After the tables are loaded, it runs some of the TPC-H
/// queries in `examples/batch/tpch/queries/rewrites` on the multi-stage engine. See the README in that folder.
public class TPCHQuickStart extends Quickstart {
  private static final String QUICKSTART_IDENTIFIER = "TPCH";
  private static final String[] TPCH_DIRECTORIES = new String[]{
      "examples/batch/tpch/customer",
      "examples/batch/tpch/lineitem",
      "examples/batch/tpch/nation",
      "examples/batch/tpch/orders",
      "examples/batch/tpch/part",
      "examples/batch/tpch/partsupp",
      "examples/batch/tpch/region",
      "examples/batch/tpch/supplier"
  };
  private static final String REWRITTEN_QUERY_FOLDER = "examples/batch/tpch/queries/rewrites/";
  private static final Map<Integer, String> SAMPLE_QUERIES = Map.of(
      1, "TPC-H Q1: pricing summary report",
      3, "TPC-H Q3: shipping priority",
      5, "TPC-H Q5: local supplier volume",
      10, "TPC-H Q10: returned item reporting");
  private static final Map<String, String> SAMPLE_QUERY_OPTIONS = Map.of("queryOptions",
      QueryOptionKey.USE_MULTISTAGE_ENGINE + "=true;" + QueryOptionKey.INFER_PARTITION_HINT + "=true");

  @Override
  public List<String> types() {
    return List.of(QUICKSTART_IDENTIFIER);
  }

  @Override
  public String[] getDefaultBatchTableDirectories() {
    return TPCH_DIRECTORIES;
  }

  @Override
  protected int getNumQuickstartRunnerServers() {
    return 1;
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    printStatus(Color.YELLOW, "***** TPC-H queries on the multi-stage engine *****");
    for (int queryNumber : new int[]{1, 3, 5, 10}) {
      runAndPrintQuery(runner, SAMPLE_QUERIES.get(queryNumber), readRewrittenQuery(queryNumber),
          SAMPLE_QUERY_OPTIONS);
    }
  }

  private static String readRewrittenQuery(int queryNumber)
      throws IOException {
    String resourcePath = REWRITTEN_QUERY_FOLDER + queryNumber + ".sql";
    try (InputStream inputStream = TPCHQuickStart.class.getClassLoader().getResourceAsStream(resourcePath)) {
      if (inputStream == null) {
        throw new IOException("Unable to find query file: " + resourcePath);
      }
      return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    }
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", QUICKSTART_IDENTIFIER));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
