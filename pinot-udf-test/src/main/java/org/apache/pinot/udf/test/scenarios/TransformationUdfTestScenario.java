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
package org.apache.pinot.udf.test.scenarios;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.udf.test.UdfExampleResult;
import org.apache.pinot.udf.test.UdfTestCluster;
import org.apache.pinot.udf.test.UdfTestScenario;


/// A test scenario where the UDF is executed as a TransformFunction.
public class TransformationUdfTestScenario extends AbstractUdfTestScenario {
  public TransformationUdfTestScenario(UdfTestCluster cluster, UdfExample.NullHandling nullHandlingMode) {
    super(cluster, nullHandlingMode);
  }

  @Override
  public String getTitle() {
    return "SSE projection (" + (getNullHandlingMode() == UdfExample.NullHandling.ENABLED ? "with" : "without")
        + " null handling)";
  }

  @Override
  public String getDescription() {
    return "This scenario tests the UDF as a projection in the SSE.";
  }

  @Override
  public Map<UdfExample, UdfExampleResult> execute(
      Udf suite,
      UdfSignature signature) {

    // language=sql
    String sqlTemplate = ""
        + "SELECT \n"
        + "  @testCol as test, \n"
        + "  @call AS result\n"
        + "FROM @table \n"
        + "WHERE @signatureCol = '@signature' \n"
        + "  AND @udfCol = '@udfName' \n"
        + "  AND @testCol = '@example' \n";

    UdfTestCluster.ExecutionContext context = new UdfTestCluster.ExecutionContext(
        getNullHandlingMode(),
        false);

    return extractResultsByCase(suite, signature, context, sqlTemplate);
  }

  /// A factory that creates an instance of this scenario with null handling enabled
  @AutoService(UdfTestScenario.Factory.class)
  public static class WithNullHandlingFactory extends UdfTestScenario.Factory.FromCluster {
    public WithNullHandlingFactory() {
      super(cluster -> new TransformationUdfTestScenario(cluster, UdfExample.NullHandling.ENABLED));
    }
  }

  /// A factory that creates an instance of this scenario with null handling disabled
  @AutoService(UdfTestScenario.Factory.class)
  public static class WithoutNullHandlingFactory extends UdfTestScenario.Factory.FromCluster {
    public WithoutNullHandlingFactory() {
      super(cluster -> new TransformationUdfTestScenario(cluster, UdfExample.NullHandling.DISABLED));
    }
  }
}
