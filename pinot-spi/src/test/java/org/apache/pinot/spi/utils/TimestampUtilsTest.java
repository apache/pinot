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
package org.apache.pinot.spi.utils;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TimestampUtilsTest {
  @Test
  public void testToTimestamp() {
    Assert.assertEquals(
        TimestampUtils.toTimestamp("2024-07-12 15:32:36.111"),
        Timestamp.valueOf("2024-07-12 15:32:36.111")
    );
    Assert.assertEquals(
        TimestampUtils.toTimestamp("2024-07-12 15:32:36"),
        Timestamp.valueOf(LocalDateTime.of(2024, 7, 12, 15, 32, 36))
    );
    Assert.assertEquals(
        TimestampUtils.toTimestamp("2024-07-12 15:32"),
        Timestamp.valueOf(LocalDateTime.of(2024, 7, 12, 15, 32))
    );
    Assert.assertEquals(
        TimestampUtils.toTimestamp("2024-07-12"),
        Timestamp.valueOf(LocalDate.of(2024, 7, 12).atStartOfDay())
    );
    Assert.assertEquals(TimestampUtils.toTimestamp("1720798356111"), new Timestamp(1720798356111L));
    Assert.assertThrows(IllegalArgumentException.class, () -> TimestampUtils.toTimestamp("July 12, 2024"));
  }

  @Test
  public void testToMillisSinceEpoch() {
    Assert.assertEquals(
        TimestampUtils.toMillisSinceEpoch("2024-07-12 15:32:36.111"),
        Timestamp.valueOf("2024-07-12 15:32:36.111").getTime()
    );
    Assert.assertEquals(
        TimestampUtils.toMillisSinceEpoch("2024-07-12 15:32:36"),
        Timestamp.valueOf(LocalDateTime.of(2024, 7, 12, 15, 32, 36)).getTime()
    );
    Assert.assertEquals(
        TimestampUtils.toMillisSinceEpoch("2024-07-12 15:32"),
        Timestamp.valueOf(LocalDateTime.of(2024, 7, 12, 15, 32)).getTime()
    );
    Assert.assertEquals(
        TimestampUtils.toMillisSinceEpoch("2024-07-12"),
        Timestamp.valueOf(LocalDate.of(2024, 7, 12).atStartOfDay()).getTime()
    );
    Assert.assertEquals(TimestampUtils.toMillisSinceEpoch("1720798356111"), 1720798356111L);
    Assert.assertThrows(IllegalArgumentException.class, () -> TimestampUtils.toMillisSinceEpoch("July 12, 2024"));
  }
}
