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
package org.apache.pinot.core.operator.filter.custom;

import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


public class CustomFilterOperatorRegistryTest {

  @AfterMethod
  public void tearDown() {
    CustomFilterOperatorRegistry.clear();
  }

  @Test
  public void testCanonicalizedLookup() {
    CustomFilterOperatorFactory factory = new TestCustomFilterOperatorFactory("LIKE_ANY");

    CustomFilterOperatorRegistry.register(factory);

    Assert.assertSame(CustomFilterOperatorRegistry.get("LIKE_ANY"), factory);
    Assert.assertSame(CustomFilterOperatorRegistry.get("like_any"), factory);
    Assert.assertSame(CustomFilterOperatorRegistry.get("likeany"), factory);
  }

  private static final class TestCustomFilterOperatorFactory implements CustomFilterOperatorFactory {
    private final String _predicateName;

    private TestCustomFilterOperatorFactory(String predicateName) {
      _predicateName = predicateName;
    }

    @Override
    public String predicateName() {
      return _predicateName;
    }

    @Override
    public BaseFilterOperator createFilterOperator(IndexSegment indexSegment, QueryContext queryContext,
        Predicate predicate, @Nullable DataSource dataSource, int numDocs) {
      return null;
    }
  }
}
