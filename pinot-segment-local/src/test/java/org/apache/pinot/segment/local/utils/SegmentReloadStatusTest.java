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
package org.apache.pinot.segment.local.utils;

import java.io.IOException;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


public class SegmentReloadStatusTest {

  @Test
  public void testConstructorExtractsExceptionDetails() {
    // Given
    String segmentName = "segment_123";
    Exception exception = new IOException("Disk full");

    // When
    SegmentReloadStatus status = new SegmentReloadStatus(segmentName, exception);

    // Then
    assertThat(status.getSegmentName()).isEqualTo(segmentName);
    assertThat(status.getErrorMessage()).isEqualTo("Disk full");
    assertThat(status.getStackTrace()).contains("java.io.IOException: Disk full");
    assertThat(status.getStackTrace()).contains("at org.apache.pinot.segment.local.utils.SegmentReloadStatusTest");
    assertThat(status.getFailedAtMs()).isGreaterThan(0);
    assertThat(status.getFailedAtMs()).isLessThanOrEqualTo(System.currentTimeMillis());
  }

  @Test
  public void testWithNullMessage() {
    // Given
    String segmentName = "segment_456";
    Exception exception = new RuntimeException((String) null);

    // When
    SegmentReloadStatus status = new SegmentReloadStatus(segmentName, exception);

    // Then
    assertThat(status.getSegmentName()).isEqualTo(segmentName);
    assertThat(status.getErrorMessage()).isNull();
    assertThat(status.getStackTrace()).contains("java.lang.RuntimeException");
    assertThat(status.getStackTrace()).isNotEmpty();
  }

  @Test
  public void testConstructorWithNullSegmentName() {
    // Given
    Exception exception = new IOException("Test");

    // When/Then
    assertThatThrownBy(() -> new SegmentReloadStatus(null, exception))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("segmentName cannot be null");
  }

  @Test
  public void testConstructorWithNullException() {
    // Given
    String segmentName = "segment_123";

    // When/Then
    assertThatThrownBy(() -> new SegmentReloadStatus(segmentName, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("exception cannot be null");
  }
}
