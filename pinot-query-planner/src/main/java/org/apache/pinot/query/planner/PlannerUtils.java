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
package org.apache.pinot.query.planner;

import java.io.PrintWriter;
import java.io.StringWriter;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDotWriter;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.externalize.RelXmlWriter;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.pinot.query.planner.explain.PinotRelJsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utilities used by planner.
 */
public class PlannerUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(PlannerUtils.class);

  private PlannerUtils() {
    // do not instantiate.
  }

  public static boolean isRootPlanFragment(int planFragmentId) {
    return planFragmentId == 0;
  }

  public static boolean isFinalPlanFragment(int planFragmentId) {
    return planFragmentId == 1;
  }

  /**
   * Like {@link RelOptUtil#dumpPlan(String, RelNode, SqlExplainFormat, SqlExplainLevel)} but uses a different json
   * writer.
   */
  public static String explainPlan(RelNode relRoot, SqlExplainFormat format, SqlExplainLevel explainLevel) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    pw.println("Execution Plan");
    RelWriter planWriter;
    switch (format) {
      case XML:
        planWriter = new RelXmlWriter(pw, explainLevel);
        break;
      case JSON:
        planWriter = new PinotRelJsonWriter();
        relRoot.explain(planWriter);
        return ((RelJsonWriter) planWriter).asString();
      case DOT:
        planWriter = new RelDotWriter(pw, explainLevel, false);
        break;
      default:
        planWriter = new RelWriterImpl(pw, explainLevel, false);
        break;
    }
    relRoot.explain(planWriter);
    pw.flush();
    return sw.toString();
  }
}
