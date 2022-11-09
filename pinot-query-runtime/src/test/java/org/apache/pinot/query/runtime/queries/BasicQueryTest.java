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
package org.apache.pinot.query.runtime.queries;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * This is a basic test for executiong QueryRunnerTestCases with various different configurations.
 */
public class BasicQueryTest extends QueryTestCaseRunner {

  private static final List<TestCase> TEST_CASE_LIST = new ArrayList<>();

  static {
    TestCase test1 = new TestCase();
    test1._name = "test1";
    test1._sql = "SELECT * FROM tbl1";
    test1._tableSchemas = ImmutableMap.of(
        "tbl1", ImmutableList.of(FieldSpec.DataType.STRING.name(), FieldSpec.DataType.INT.name()));
    test1._tableInputs = ImmutableMap.of(
        "tbl1", ImmutableList.of(new Object[]{"foo", 1}, new Object[]{"bar", 2}));
    TEST_CASE_LIST.add(test1);
  }

  @Override
  public List<TestCase> getTestCases() {
    return TEST_CASE_LIST;
  }
}
