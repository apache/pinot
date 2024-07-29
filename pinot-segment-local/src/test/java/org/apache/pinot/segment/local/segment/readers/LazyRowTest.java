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
package org.apache.pinot.segment.local.segment.readers;

import java.util.Arrays;
import java.util.HashSet;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class LazyRowTest {

  private DataSource _col1Datasource;
  private Dictionary _col2Dictionary;

  @Test
  public void testIsNullField() {
    IndexSegment segment = getMockSegment();
    LazyRow lazyRow = spy(new LazyRow());
    lazyRow.init(segment, 1);

    // first invocation will read from disk
    assertTrue(lazyRow.isNullValue("col1"));

    assertTrue(lazyRow.isNullValue("col1"));
    // only one disk read.
    verify(lazyRow, times(1)).getValue("col1");

    // should return false when value exists for a field in an indexed row
    assertFalse(lazyRow.isNullValue("col2"));
  }

  @Test
  public void testGetValue() {
    IndexSegment segment = getMockSegment();
    LazyRow lazyRow = spy(new LazyRow());
    lazyRow.init(segment, 1);

    // should return persisted value
    assertEquals(lazyRow.getValue("col2"), "val2");

    // second invocation should read from LazyRow._nullValueFields
    assertEquals(lazyRow.getValue("col2"), "val2");
    // only one disk read
    verify(_col2Dictionary, times(1)).get(1);

    assertNull(lazyRow.getValue("col1"));
  }

  @Test
  public void testGetColumnNames() {
    IndexSegment segment = getMockSegment();
    LazyRow lazyRow = new LazyRow();
    lazyRow.init(segment, 1);
    HashSet<String> columnNames = new HashSet<>(Arrays.asList("col1", "col2"));
    when(segment.getColumnNames()).thenReturn(columnNames);

    assertEquals(lazyRow.getColumnNames(), columnNames);
  }

  private IndexSegment getMockSegment() {
    IndexSegment segment = mock(IndexSegment.class);
    _col1Datasource = mock(DataSource.class);
    DataSource col2Datasource = mock(DataSource.class);
    when(segment.getDataSource("col1")).thenReturn(_col1Datasource);
    when(segment.getDataSource("col2")).thenReturn(col2Datasource);

    NullValueVectorReader col1NullVectorReader = mock(NullValueVectorReader.class);
    when(col1NullVectorReader.isNull(1)).thenReturn(true);
    NullValueVectorReader col2NullVectorReader = mock(NullValueVectorReader.class);
    when(col2NullVectorReader.isNull(1)).thenReturn(false);
    when(_col1Datasource.getNullValueVector()).thenReturn(col1NullVectorReader);
    when(col2Datasource.getNullValueVector()).thenReturn(col2NullVectorReader);

    ForwardIndexReader col1ForwardIndexReader = mock(ForwardIndexReader.class);
    when(col1ForwardIndexReader.isSingleValue()).thenReturn(true);
    ForwardIndexReader col2ForwardIndexReader = mock(ForwardIndexReader.class);
    when(col2ForwardIndexReader.isSingleValue()).thenReturn(true);
    when(_col1Datasource.getForwardIndex()).thenReturn(col1ForwardIndexReader);
    when(col2Datasource.getForwardIndex()).thenReturn(col2ForwardIndexReader);
    when(col2ForwardIndexReader.getDictId(eq(1), any())).thenReturn(1);

    Dictionary col1Dictionary = mock(Dictionary.class);
    when(_col1Datasource.getDictionary()).thenReturn(col1Dictionary);
    _col2Dictionary = mock(Dictionary.class);
    when(col2Datasource.getDictionary()).thenReturn(_col2Dictionary);
    when(_col2Dictionary.get(1)).thenReturn("val2");

    return segment;
  }
}
