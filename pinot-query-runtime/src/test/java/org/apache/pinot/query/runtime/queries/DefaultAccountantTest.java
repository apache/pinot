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

import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.trace.Tracing;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DefaultAccountantTest extends QueryRunnerAccountingTest {
  @Override
  protected ThreadResourceUsageAccountant getThreadResourceUsageAccountant() {
    return new Tracing.DefaultThreadResourceUsageAccountant();
  }

  @Test
  void testWithDefaultThreadAccountant() {
    Tracing.DefaultThreadResourceUsageAccountant accountant = new Tracing.DefaultThreadResourceUsageAccountant();
    try (MockedStatic<Tracing> tracing = Mockito.mockStatic(Tracing.class, Mockito.CALLS_REAL_METHODS)) {
      tracing.when(Tracing::getThreadAccountant).thenReturn(accountant);

      ResultTable resultTable = queryRunner("SELECT * FROM a LIMIT 2", false).getResultTable();
      Assert.assertEquals(resultTable.getRows().size(), 2);

      ThreadResourceUsageAccountant threadAccountant = Tracing.getThreadAccountant();
      Assert.assertTrue(threadAccountant.getThreadResources().isEmpty());
      Assert.assertTrue(threadAccountant.getQueryResources().isEmpty());
    }
  }
}
