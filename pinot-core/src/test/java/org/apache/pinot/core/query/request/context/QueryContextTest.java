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
package org.apache.pinot.core.query.request.context;

import java.lang.ref.WeakReference;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;


public class QueryContextTest {

  @Test
  public void testSharedValueReferencingQueryContextDoesNotLeak()
      throws InterruptedException {
    WeakReference<QueryContext> queryContextRef = createQueryContextWithSelfReferencingSharedValue();

    // A shared value that references its query context must not keep the query context reachable
    for (int i = 0; i < 100 && queryContextRef.get() != null; i++) {
      System.gc();
      Thread.sleep(10);
    }
    assertNull(queryContextRef.get(), "QueryContext is still reachable after GC");
  }

  private static WeakReference<QueryContext> createQueryContextWithSelfReferencingSharedValue() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable");
    SelfReferencingValue value =
        queryContext.getOrComputeSharedValue(SelfReferencingValue.class, "key", k -> new SelfReferencingValue(
            queryContext));
    assertSame(queryContext.getOrComputeSharedValue(SelfReferencingValue.class, "key", k -> null), value);
    return new WeakReference<>(queryContext);
  }

  private static final class SelfReferencingValue {
    @SuppressWarnings("unused")
    private final QueryContext _queryContext;

    SelfReferencingValue(QueryContext queryContext) {
      _queryContext = queryContext;
    }
  }
}
