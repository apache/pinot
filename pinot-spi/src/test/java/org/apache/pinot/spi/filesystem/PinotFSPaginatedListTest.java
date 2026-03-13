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
package org.apache.pinot.spi.filesystem;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/**
 * Tests for the 4-arg listFilesWithMetadata: default method guard clause and NoClosePinotFS delegation.
 */
public class PinotFSPaginatedListTest {

  private static final URI TEST_URI = URI.create("s3://bucket/data/");
  private static final Predicate<String> ACCEPT_ALL = path -> true;

  // --- PinotFS default method: maxResults <= 0 guard ---

  @Test
  public void testDefaultMethodMaxResultsZeroReturnsEmpty() throws IOException {
    final PinotFS mockFS = mock(PinotFS.class);
    when(mockFS.listFilesWithMetadata(any(URI.class), anyBoolean(), any(), anyInt()))
        .thenCallRealMethod();

    final List<FileMetadata> result = mockFS.listFilesWithMetadata(TEST_URI, true, ACCEPT_ALL, 0);

    assertEquals(result.size(), 0);
    // 2-arg method should never be called when maxResults <= 0
    verify(mockFS, never()).listFilesWithMetadata(any(URI.class), anyBoolean());
  }

  @Test
  public void testDefaultMethodMaxResultsNegativeReturnsEmpty() throws IOException {
    final PinotFS mockFS = mock(PinotFS.class);
    when(mockFS.listFilesWithMetadata(any(URI.class), anyBoolean(), any(), anyInt()))
        .thenCallRealMethod();

    final List<FileMetadata> result = mockFS.listFilesWithMetadata(TEST_URI, true, ACCEPT_ALL, -10);

    assertEquals(result.size(), 0);
    verify(mockFS, never()).listFilesWithMetadata(any(URI.class), anyBoolean());
  }

  @Test
  public void testDefaultMethodPositiveMaxResultsDelegatesToTwoArg() throws IOException {
    final PinotFS mockFS = mock(PinotFS.class);
    when(mockFS.listFilesWithMetadata(any(URI.class), anyBoolean(), any(), anyInt()))
        .thenCallRealMethod();
    when(mockFS.listFilesWithMetadata(any(URI.class), anyBoolean()))
        .thenReturn(Arrays.asList(
            new FileMetadata.Builder().setFilePath("s3://bucket/data/f1.parquet")
                .setLength(100).setIsDirectory(false).build(),
            new FileMetadata.Builder().setFilePath("s3://bucket/data/f2.parquet")
                .setLength(200).setIsDirectory(false).build()
        ));

    final List<FileMetadata> result = mockFS.listFilesWithMetadata(TEST_URI, true, ACCEPT_ALL, 10);

    assertEquals(result.size(), 2);
    verify(mockFS).listFilesWithMetadata(any(URI.class), anyBoolean());
  }

  // --- NoClosePinotFS delegation ---

  @SuppressWarnings("unchecked")
  @Test
  public void testNoClosePinotFSDelegatesToUnderlyingFS() throws IOException {
    final PinotFS mockDelegate = mock(PinotFS.class);
    final List<FileMetadata> expected = Arrays.asList(
        new FileMetadata.Builder().setFilePath("s3://bucket/data/f1.parquet")
            .setLength(100).setIsDirectory(false).build()
    );
    when(mockDelegate.listFilesWithMetadata(any(URI.class), anyBoolean(), any(), anyInt()))
        .thenReturn(expected);

    final NoClosePinotFS noCloseFS = new NoClosePinotFS(mockDelegate);
    final Predicate<String> filter = path -> path.endsWith(".parquet");
    final List<FileMetadata> result = noCloseFS.listFilesWithMetadata(TEST_URI, true, filter, 5);

    assertEquals(result, expected);
    verify(mockDelegate).listFilesWithMetadata(TEST_URI, true, filter, 5);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testNoClosePinotFSPassesExactArguments() throws IOException {
    final PinotFS mockDelegate = mock(PinotFS.class);
    when(mockDelegate.listFilesWithMetadata(any(URI.class), anyBoolean(), any(), anyInt()))
        .thenReturn(Arrays.asList());

    final NoClosePinotFS noCloseFS = new NoClosePinotFS(mockDelegate);
    final URI uri = URI.create("gs://my-bucket/prefix/");
    final Predicate<String> filter = path -> false;
    noCloseFS.listFilesWithMetadata(uri, false, filter, 42);

    verify(mockDelegate).listFilesWithMetadata(uri, false, filter, 42);
    // 2-arg should NOT be called — delegation should go directly to the optimized 4-arg on delegate
    verify(mockDelegate, never()).listFilesWithMetadata(any(URI.class), anyBoolean());
  }
}
