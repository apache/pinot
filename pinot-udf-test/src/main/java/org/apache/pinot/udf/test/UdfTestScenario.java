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
package org.apache.pinot.udf.test;

import java.util.Map;
import java.util.function.Function;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfSignature;

/// A scenario for testing [UDFs][Udf] (User Defined Functions).
///
/// This interface is used to define _a way to test a UDF_ with a given set of examples.
/// This is necessary because Pinot supports a wide range of query engines and even the same engine may use UDFs in
/// different ways. For example, it is not the same to test a UDF in the context of a SSE block transform, a MSE
/// intermediate projection or a row transform during ingestion.
public interface UdfTestScenario {

  /// A title for the scenario, used in reports.
  String getTitle();

  /// A description of the scenario, used in reports.
  String getDescription();

  /// Execute the test scenario for the given UDF suite and signature.
  Map<UdfExample, UdfExampleResult> execute(Udf suite, UdfSignature signature);

  /// A factory interface to create scenarios.
  /// This is mainly used to be able to register these scenarios using ServiceLoader, so that they can be
  /// discovered and used by the UDF test framework.
  interface Factory {
    UdfTestScenario create(UdfTestCluster cluster);

    class FromCluster implements Factory {
      private final Function<UdfTestCluster, UdfTestScenario> _factoryFun;

      public FromCluster(Function<UdfTestCluster, UdfTestScenario> factoryFun) {
        _factoryFun = factoryFun;
      }

      @Override
      public UdfTestScenario create(UdfTestCluster cluster) {
        return _factoryFun.apply(cluster);
      }
    }
  }
}
