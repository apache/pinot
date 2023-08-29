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
package org.apache.pinot.core.operator.transform.function;

import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;


public class LiteralTransformFunctionTest {
  private static final int NUM_DOCS = 100;
  private AutoCloseable _mocks;

  @Mock
  private ProjectionBlock _projectionBlock;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    when(_projectionBlock.getNumDocs()).thenReturn(NUM_DOCS);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void testLiteralTransformFunction() {
    LiteralTransformFunction trueBoolean = new LiteralTransformFunction(new LiteralContext(DataType.BOOLEAN, true));
    Assert.assertEquals(trueBoolean.getResultMetadata().getDataType(), DataType.BOOLEAN);
    Assert.assertEquals(trueBoolean.getBooleanLiteral(), true);
    LiteralTransformFunction falseBoolean = new LiteralTransformFunction(new LiteralContext(DataType.BOOLEAN, false));
    Assert.assertEquals(falseBoolean.getResultMetadata().getDataType(), DataType.BOOLEAN);
    Assert.assertEquals(falseBoolean.getBooleanLiteral(), false);
    LiteralTransformFunction nullLiteral = new LiteralTransformFunction(new LiteralContext(DataType.UNKNOWN, true));
    Assert.assertEquals(nullLiteral.getStringLiteral(), "null");
  }

  @Test
  public void testNullTransform() {
    LiteralTransformFunction nullLiteral = new LiteralTransformFunction(new LiteralContext(DataType.UNKNOWN, true));
    Assert.assertEquals(nullLiteral.getStringLiteral(), "null");
    RoaringBitmap bitmap = nullLiteral.getNullBitmap(_projectionBlock);
    RoaringBitmap expectedBitmap = new RoaringBitmap();
    expectedBitmap.add(0L, NUM_DOCS);
    Assert.assertEquals(bitmap, expectedBitmap);
    int[] intValues = nullLiteral.transformToIntValuesSV(_projectionBlock);
    Assert.assertEquals(intValues.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertEquals(intValues[i], 0);
    }
    Assert.assertEquals(nullLiteral.getNullBitmap(_projectionBlock), expectedBitmap);
  }

  @Test
  public void testIsNullLiteralTransform() {
    LiteralTransformFunction nullLiteral =
        new LiteralTransformFunction(new LiteralContext(DataType.UNKNOWN, null));
    Assert.assertTrue(nullLiteral.isNull());

    LiteralTransformFunction nullLiteralWITHBooleanValue =
        new LiteralTransformFunction(new LiteralContext(DataType.UNKNOWN, true));
    Assert.assertTrue(nullLiteralWITHBooleanValue.isNull());

    LiteralTransformFunction trueLiteral =
        new LiteralTransformFunction(new LiteralContext(DataType.BOOLEAN, true));
    Assert.assertFalse(trueLiteral.isNull());

    LiteralTransformFunction stringNullLiteral =
        new LiteralTransformFunction(new LiteralContext(DataType.STRING, null));
    Assert.assertFalse(stringNullLiteral.isNull());
  }
}
