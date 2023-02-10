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
package org.apache.pinot.common.request.context;

import java.math.BigDecimal;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.Test;


public class LiteralContextTest {
  @Test
  public void testNullLiteralContext() {
    // Create literal context from thrift
    Literal literal = new Literal();
    literal.setNullValue(true);
    LiteralContext nullContext1 = new LiteralContext(literal);
    Assert.assertEquals(nullContext1.getValue(), null);
    Assert.assertEquals(nullContext1.toString(), "'null'");
    Assert.assertEquals(nullContext1.getBigDecimalValue(FieldSpec.DataType.UNKNOWN), BigDecimal.ZERO);
    // Create literal context from object and type
    LiteralContext nullContext2 = new LiteralContext(FieldSpec.DataType.UNKNOWN, null);
    Assert.assertEquals(nullContext2.getValue(), null);
    Assert.assertEquals(nullContext2.toString(), "'null'");
    Assert.assertEquals(nullContext2.getBigDecimalValue(FieldSpec.DataType.UNKNOWN), BigDecimal.ZERO);
    // Check different literal objects are equal and have same hash code.
    Assert.assertTrue(nullContext1.equals(nullContext2));
    Assert.assertTrue(nullContext1.hashCode() == nullContext2.hashCode());
  }
}
