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
package org.apache.pinot.segment.spi.creator;

import java.io.File;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class IndexCreationContextTest {

  @Test
  public void testForwardEncodingIsIndependentOfDictionaryPresence() {
    ColumnMetadata columnMetadata = Mockito.mock(ColumnMetadata.class);
    FieldSpec fieldSpec = new DimensionFieldSpec("rawCol", FieldSpec.DataType.STRING, true);
    Mockito.when(columnMetadata.getFieldSpec()).thenReturn(fieldSpec);
    Mockito.when(columnMetadata.isSorted()).thenReturn(false);
    Mockito.when(columnMetadata.getCardinality()).thenReturn(1);
    Mockito.when(columnMetadata.getTotalNumberOfEntries()).thenReturn(1);
    Mockito.when(columnMetadata.getTotalDocs()).thenReturn(1);
    Mockito.when(columnMetadata.hasDictionary()).thenReturn(true);
    Mockito.when(columnMetadata.getForwardIndexEncoding()).thenReturn(IndexCreationContext.ForwardIndexEncoding.RAW);
    Mockito.when(columnMetadata.getMinValue()).thenReturn("a");
    Mockito.when(columnMetadata.getMaxValue()).thenReturn("z");
    Mockito.when(columnMetadata.getMaxNumberOfMultiValues()).thenReturn(1);

    IndexCreationContext.Common context = IndexCreationContext.builder()
        .withIndexDir(new File("."))
        .withColumnMetadata(columnMetadata)
        .withForwardIndexEncoding(IndexCreationContext.ForwardIndexEncoding.RAW)
        .build();

    assertTrue(context.hasDictionary(), "Dictionary presence should still reflect column metadata");
    assertEquals(context.getForwardIndexEncoding(), IndexCreationContext.ForwardIndexEncoding.RAW,
        "Forward encoding should not be inferred from dictionary presence");
  }

  @Test
  public void testDefaultForwardEncodingIsRaw() {
    IndexCreationContext.Common context = IndexCreationContext.builder()
        .withIndexDir(new File("."))
        .withFieldSpec(new DimensionFieldSpec("rawCol", FieldSpec.DataType.STRING, true))
        .build();

    assertEquals(context.getForwardIndexEncoding(), IndexCreationContext.ForwardIndexEncoding.RAW);
  }
}
