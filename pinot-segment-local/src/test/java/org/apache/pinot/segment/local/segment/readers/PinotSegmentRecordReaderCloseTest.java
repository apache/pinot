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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.pinot.segment.spi.IndexSegment;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


/// Regression coverage for [PinotSegmentRecordReader#close()]: an [IOException] from one column reader must not
/// leak the rest, and must not skip destroying the owned segment.
public class PinotSegmentRecordReaderCloseTest {

  @Test
  public void testCloseClosesEveryReaderEvenWhenOneThrows()
      throws Exception {
    PinotSegmentColumnReader throwingReader = mock(PinotSegmentColumnReader.class);
    doThrow(new IOException("boom")).when(throwingReader).close();
    PinotSegmentColumnReader okReader = mock(PinotSegmentColumnReader.class);

    Map<String, PinotSegmentColumnReader> columnReaderMap = new LinkedHashMap<>();
    columnReaderMap.put("throwing", throwingReader);
    columnReaderMap.put("ok", okReader);

    IndexSegment indexSegment = mock(IndexSegment.class);
    PinotSegmentRecordReader reader = new PinotSegmentRecordReader();
    setField(reader, "_columnReaderMap", columnReaderMap);
    setField(reader, "_indexSegment", indexSegment);
    setField(reader, "_destroySegmentOnClose", true);

    IOException thrown = expectThrows(IOException.class, reader::close);
    assertEquals(thrown.getMessage(), "boom");
    verify(okReader, times(1)).close();
    verify(indexSegment, times(1)).destroy();
  }

  @Test
  public void testCloseSuppressesLaterExceptionsOnFirst()
      throws Exception {
    PinotSegmentColumnReader firstThrowingReader = mock(PinotSegmentColumnReader.class);
    doThrow(new IOException("first")).when(firstThrowingReader).close();
    PinotSegmentColumnReader secondThrowingReader = mock(PinotSegmentColumnReader.class);
    doThrow(new IOException("second")).when(secondThrowingReader).close();

    Map<String, PinotSegmentColumnReader> columnReaderMap = new LinkedHashMap<>();
    columnReaderMap.put("first", firstThrowingReader);
    columnReaderMap.put("second", secondThrowingReader);

    PinotSegmentRecordReader reader = new PinotSegmentRecordReader();
    setField(reader, "_columnReaderMap", columnReaderMap);

    IOException thrown = expectThrows(IOException.class, reader::close);
    assertEquals(thrown.getMessage(), "first");
    assertEquals(thrown.getSuppressed().length, 1);
    assertEquals(thrown.getSuppressed()[0].getMessage(), "second");
  }

  private static void setField(Object target, String fieldName, Object value)
      throws Exception {
    Field field = PinotSegmentRecordReader.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
