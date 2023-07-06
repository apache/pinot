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

package org.apache.pinot.spi.data;

import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class DateTimeGranularitySpecTest {

  @Test
  public void testDateTimeGranularitySpec() {
    // Old format
    DateTimeGranularitySpec dateTimeGranularitySpec = new DateTimeGranularitySpec("1:HOURS");
    assertEquals(dateTimeGranularitySpec.getSize(), 1);
    assertEquals(dateTimeGranularitySpec.getTimeUnit(), TimeUnit.HOURS);
    assertEquals(dateTimeGranularitySpec.granularityToMillis(), 3600000);

    dateTimeGranularitySpec = new DateTimeGranularitySpec("15:MINUTES");
    assertEquals(dateTimeGranularitySpec.getSize(), 15);
    assertEquals(dateTimeGranularitySpec.getTimeUnit(), TimeUnit.MINUTES);
    assertEquals(dateTimeGranularitySpec.granularityToMillis(), 900000);

    dateTimeGranularitySpec = new DateTimeGranularitySpec("1:MILLISECONDS");
    assertEquals(dateTimeGranularitySpec.getSize(), 1);
    assertEquals(dateTimeGranularitySpec.getTimeUnit(), TimeUnit.MILLISECONDS);
    assertEquals(dateTimeGranularitySpec.granularityToMillis(), 1);

    // New format
    dateTimeGranularitySpec = new DateTimeGranularitySpec("HOURS|1");
    assertEquals(dateTimeGranularitySpec.getSize(), 1);
    assertEquals(dateTimeGranularitySpec.getTimeUnit(), TimeUnit.HOURS);
    assertEquals(dateTimeGranularitySpec.granularityToMillis(), 3600000);

    dateTimeGranularitySpec = new DateTimeGranularitySpec("MINUTES|15");
    assertEquals(dateTimeGranularitySpec.getSize(), 15);
    assertEquals(dateTimeGranularitySpec.getTimeUnit(), TimeUnit.MINUTES);
    assertEquals(dateTimeGranularitySpec.granularityToMillis(), 900000);

    dateTimeGranularitySpec = new DateTimeGranularitySpec("MILLISECONDS|1");
    assertEquals(dateTimeGranularitySpec.getSize(), 1);
    assertEquals(dateTimeGranularitySpec.getTimeUnit(), TimeUnit.MILLISECONDS);
    assertEquals(dateTimeGranularitySpec.granularityToMillis(), 1);

    assertThrows(IllegalArgumentException.class, () -> new DateTimeGranularitySpec("DAY:1"));
    assertThrows(IllegalArgumentException.class, () -> new DateTimeGranularitySpec("1|DAY"));
    assertThrows(IllegalArgumentException.class, () -> new DateTimeGranularitySpec("DAY:DAY"));
    assertThrows(IllegalArgumentException.class, () -> new DateTimeGranularitySpec("1:1"));
    assertThrows(IllegalArgumentException.class, () -> new DateTimeGranularitySpec("1:DAY:EPOCH"));
  }
}
