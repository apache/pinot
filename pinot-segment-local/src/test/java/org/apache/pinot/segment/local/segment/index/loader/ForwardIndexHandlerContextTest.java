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
package org.apache.pinot.segment.local.segment.index.loader;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;


/// Unit tests for the resource lifecycle of [ForwardIndexReaderContext] instances created inside
/// [ForwardIndexHandler] rewrite helpers. Verifies that a helper closes the context it creates on both the success
/// and the failure path, without closing the reader or creator it does not own. Single-threaded; no shared state.
public class ForwardIndexHandlerContextTest {

  @Test
  public void testRewriteHelperClosesOwnedContextOnSuccess() throws Exception {
    ForwardIndexReaderContext context = mock(ForwardIndexReaderContext.class);
    ForwardIndexReader<ForwardIndexReaderContext> reader = mockReader(context);
    ForwardIndexCreator creator = mock(ForwardIndexCreator.class);
    when(reader.getDictId(0, context)).thenReturn(7);

    assertEquals(invokeDictRewriteHelper(reader, creator), 1L);

    verify(context).close();
    verify(reader, never()).close();
    verify(creator, never()).close();
  }

  @Test
  public void testRewriteHelperClosesOwnedContextOnFailure() throws Exception {
    ForwardIndexReaderContext context = mock(ForwardIndexReaderContext.class);
    ForwardIndexReader<ForwardIndexReaderContext> reader = mockReader(context);
    ForwardIndexCreator creator = mock(ForwardIndexCreator.class);
    IllegalStateException failure = new IllegalStateException("read failed");
    when(reader.getDictId(0, context)).thenThrow(failure);

    InvocationTargetException exception = expectThrows(InvocationTargetException.class,
        () -> invokeDictRewriteHelper(reader, creator));
    assertSame(exception.getCause(), failure);

    verify(context).close();
    verify(reader, never()).close();
    verify(creator, never()).close();
  }

  @SuppressWarnings("unchecked")
  private static ForwardIndexReader<ForwardIndexReaderContext> mockReader(ForwardIndexReaderContext context) {
    ForwardIndexReader<ForwardIndexReaderContext> reader = mock(ForwardIndexReader.class);
    when(reader.createContext()).thenReturn(context);
    when(reader.isSingleValue()).thenReturn(true);
    return reader;
  }

  private static long invokeDictRewriteHelper(ForwardIndexReader<ForwardIndexReaderContext> reader,
      ForwardIndexCreator creator) throws Exception {
    Method method = ForwardIndexHandler.class.getDeclaredMethod("forwardIndexReadDictWriteDictHelper",
        ForwardIndexReader.class, ForwardIndexCreator.class, int.class);
    method.setAccessible(true);
    return (long) method.invoke(null, reader, creator, 1);
  }
}
