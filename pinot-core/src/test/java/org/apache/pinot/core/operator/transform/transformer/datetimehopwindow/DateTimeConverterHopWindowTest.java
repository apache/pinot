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

package org.apache.pinot.core.operator.transform.transformer.datetimehopwindow;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.core.operator.transform.transformer.datetimehop.BaseDateTimeWindowHopTransformer;
import org.apache.pinot.core.operator.transform.transformer.datetimehop.DateTimeWindowHopTransformerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DateTimeConverterHopWindowTest {
  @SuppressWarnings("unchecked")
  @Test(dataProvider = "testDateTimeHopWindowConversion")
  public void testDateTimeHopWindowConversion(String inputFormat, String outputFormat, String outputGranularity,
      String windowGranularity, Object input, Object expected) {
    BaseDateTimeWindowHopTransformer converter =
        DateTimeWindowHopTransformerFactory.getDateTimeTransformer(inputFormat, outputFormat, outputGranularity,
            windowGranularity);
    int length;
    Object output;
    if (expected instanceof long[][]) {
      length = ((long[][]) expected).length;
      output = new long[length][];
      for (int i = 0; i < length; i++) {
        ((long[][]) output)[i] = new long[((long[][]) expected)[i].length];
      }
    } else {
      length = ((String[][]) expected).length;
      output = new String[length][];
    }
    converter.transform(input, output, length);
    Assert.assertEquals(output, expected);
  }

  @DataProvider(name = "testDateTimeHopWindowConversion")
  public Object[][] testDateTimeHopWindowConversion() {
    List<Object[]> entries = new ArrayList<>();

    /*************** Epoch to Epoch ***************/
    {
      // Test bucketing to 15 minutes with 1h hop window, for one value
      long[] input = {
          1696587946000L /* Fri Oct 06 2023 10:25:46 GMT+0000 */
      };
      long[][] expected = {
          new long[]{
              1696587300000L /* Fri Oct 06 2023 10:15:00 GMT+0000 */, 1696586400000L /* Fri Oct 06 2023 10:00:00
              GMT+0000 */, 1696585500000L /* Fri Oct 06 2023 09:45:00 GMT+0000 */, 1696584600000L /* Fri Oct 06 2023
              09:30:00 GMT+0000 */,
          }
      };
      entries.add(new Object[]{
          "EPOCH|MILLISECONDS", "EPOCH|MILLISECONDS", "MINUTES|15", "HOURS", input, expected
      });
    }
    {
      // Test bucketing to 15 minutes with 1h hop window, multiple values
      long[] input = {
          1696587946000L /* Fri Oct 06 2023 10:25:46 GMT+0000 */, 1693998340000L /* Wed Sep 06 2023 11:05:40 GMT+0000
           */,
      };
      long[][] expected = {
          new long[]{
              1696587300000L /* Fri Oct 06 2023 10:15:00 GMT+0000 */, 1696586400000L /* Fri Oct 06 2023 10:00:00
              GMT+0000 */, 1696585500000L /* Fri Oct 06 2023 09:45:00 GMT+0000 */, 1696584600000L /* Fri Oct 06 2023
              09:30:00 GMT+0000 */,
          }, new long[]{
          1693998000000L /* Fri Oct 06 2023 10:15:00 GMT+0000 */, 1693997100000L /* Fri Oct 06 2023 10:00:00 GMT+0000
           */, 1693996200000L /* Fri Oct 06 2023 09:45:00 GMT+0000 */, 1693995300000L /* Fri Oct 06 2023 09:30:00
           GMT+0000 */,
      }
      };
      entries.add(new Object[]{
          "EPOCH|MILLISECONDS", "EPOCH|MILLISECONDS", "MINUTES|15", "HOURS", input, expected
      });
    }
    {
      // Test bucketing with conversion to hours with 1h hop window, 15m bucketing
      long[] input = {
          1696587946000L /* Fri Oct 06 2023 10:25:46 GMT+0000 */
      };
      long[][] expected = {
          new long[]{
              471274L /* Fri Oct 06 2023 10:00:00 GMT+0000 */, 471274L /* Fri Oct 06 2023 10:00:00 GMT+0000 */,
              471273L /* Fri Oct 06 2023 09:00:00 GMT+0000 */, 471273L /* Fri Oct 06 2023 09:00:00 GMT+0000 */,
          }
      };
      entries.add(new Object[]{"EPOCH|MILLISECONDS", "EPOCH|HOURS", "MINUTES|15", "HOURS", input, expected});
    }
    {
      // Test bucketing with conversion to 15 min with 1h hop window, 15m bucketing
      long[] input = {
          1696587946000L /* Fri Oct 06 2023 10:25:46 GMT+0000 */
      };
      long[][] expected = {
          new long[]{
              1885097L /* Fri Oct 06 2023 10:15:00 GMT+0000 */, 1885096L /* Fri Oct 06 2023 10:00:00 GMT+0000 */,
              1885095L /* Fri Oct 06 2023 09:45:00 GMT+0000 */, 1885094L /* Fri Oct 06 2023 09:30:00 GMT+0000 */,
          }
      };
      entries.add(new Object[]{"EPOCH|MILLISECONDS", "EPOCH|MINUTES|15", "MINUTES|15", "HOURS", input, expected});
    }
    {
      {
        // Test bucketing to 1hour with 15m hop window. Since there is no intersection - empty array is expected
        long[] input = {
            1696587946000L /* Fri Oct 06 2023 10:25:46 GMT+0000 */
        };
        long[][] expected = {
            new long[]{}
        };
        entries.add(new Object[]{
            "EPOCH|MILLISECONDS", "EPOCH|MILLISECONDS", "MINUTES|60", "MINUTES|15", input, expected
        });
      }
    }
    {
      {
        // Test bucketing with non-aligned window
        long[] input = {
            1696587946000L /* Fri Oct 06 2023 10:25:46 GMT+0000 */
        };
        long[][] expected = {
            new long[]{
                1696587300000L /* Fri Oct 06 2023 10:15:00 GMT+0000 */, 1696586400000L /* Fri Oct 06 2023 10:00:00
                GMT+0000 */, 1696585500000L /* Fri Oct 06 2023 09:45:00 GMT+0000 */,
            }
        };
        entries.add(new Object[]{
            "EPOCH|MILLISECONDS", "EPOCH|MILLISECONDS", "MINUTES|15", "MINUTES|55", input, expected
        });
      }
    }
    /*************** Epoch to SDF ***************/
    {
      {
        // Test conversion from millis since epoch to simple date format (GMT timezone)
        long[] input = {
            1696587946000L /* Fri Oct 06 2023 10:25:46 GMT+0000 */
        };
        String[][] expected = {
            {"2023-10-06-10:15", "2023-10-06-10:00", "2023-10-06-09:45", "2023-10-06-09:30"}
        };
        entries.add(new Object[]{
            "EPOCH|MILLISECONDS", "SIMPLE_DATE_FORMAT|yyyy-MM-dd-HH:mm|GMT", "MINUTES|15", "HOURS|1",
            input, expected
        });
      }
    }
    {
      {
        // Test multiple values conversion from millis since epoch to simple date format (GMT timezone)
        long[] input = {
            1696587946000L, /* Fri Oct 06 2023 10:25:46 GMT+0000 */
            1696582946000L /* Fri Oct 06 2023 09:02:26 GMT+0000 */
        };
        String[][] expected = {
            {"2023-10-06-10:15", "2023-10-06-10:00", "2023-10-06-09:45", "2023-10-06-09:30"}, {
            "2023-10-06-09:00", "2023-10-06-08:45", "2023-10-06-08:30", "2023-10-06-08:15"
        }
        };
        entries.add(new Object[]{
            "EPOCH|MILLISECONDS", "SIMPLE_DATE_FORMAT|yyyy-MM-dd-HH:mm|GMT", "MINUTES|15", "HOURS|1", input, expected
        });
      }
    }
    {
      {
        // Test single value conversion from hours to simple date format (Los Angeles timezone)
        long[] input = {
            471274L, /* Fri Oct 06 2023 10:00:00 GMT+0000 */
        };
        String[][] expected = {
            {"2023-10-06-10:00", "2023-10-06-09:45", "2023-10-06-09:30", "2023-10-06-09:15"}
        };
        entries.add(new Object[]{
            "EPOCH|HOURS", "SIMPLE_DATE_FORMAT|yyyy-MM-dd-HH:mm|GMT", "MINUTES|15", "HOURS|1", input, expected
        });
      }
    }
    {
      {
        // Test single value conversion from 2 hours to simple date format (Los Angeles timezone)
        long[] input = {
            235637L, /* Fri Oct 06 2023 10:00:00 GMT+0000 */
        };
        String[][] expected = {
            {"2023-10-06-10:00", "2023-10-06-09:45", "2023-10-06-09:30", "2023-10-06-09:15"}
        };
        entries.add(new Object[]{
            "EPOCH|HOURS|2", "SIMPLE_DATE_FORMAT|yyyy-MM-dd-HH:mm|GMT", "MINUTES|15", "HOURS|1", input, expected
        });
      }
    }

    /*************** SDF to EPOCH ***************/
    {
      // Test conversion from simple date format (GMT timezone) to millis since epoch with 1h window and 15m hop
      String[] input = {
          "2023-10-06 10:25:46"
      };
      long[][] expected = {
          {
              1696587300000L /* Fri Oct 06 2023 10:00:00 GMT+0000 */, 1696586400000L /* Fri Oct 06 2023 09:45:00
              GMT+0000 */, 1696585500000L /* Fri Oct 06 2023 09:30:00 GMT+0000 */, 1696584600000L /* Fri Oct 06 2023
              09:15:00 GMT+0000 */
          }
      };
      entries.add(new Object[]{
          "SIMPLE_DATE_FORMAT|yyyy-MM-dd HH:mm:ss|GMT", "EPOCH|MILLISECONDS|1", "MINUTES|15", "MINUTES|60",
          input, expected
      });
    }
    {
      // Test conversion from simple date format (GMT timezone) to millis since epoch with 1h window
      // and 15m hop with 1:HOURS input granularity
      String[] input = {
          "2023-10-06 10:25:46"
      };
      long[][] expected = {
          {
              1696587300000L /* Fri Oct 06 2023 10:00:00 GMT+0000 */, 1696586400000L /* Fri Oct 06 2023 09:45:00
              GMT+0000 */, 1696585500000L /* Fri Oct 06 2023 09:30:00 GMT+0000 */, 1696584600000L /* Fri Oct 06 2023
              09:15:00 GMT+0000 */
          }
      };
      entries.add(new Object[]{
          "SIMPLE_DATE_FORMAT|yyyy-MM-dd HH:mm:ss|GMT", "EPOCH|MILLISECONDS|1", "MINUTES|15", "MINUTES|60",
          input, expected
      });
    }

    /*************** SDF to SDF ***************/
    {
      // Test conversion from one simple date format to another with 1h window and 15m hop
      String[] input = {
          "2023-10-06 10:12:46"
      };
      String[][] expected = {
          {"2023-10-06 10:00", "2023-10-06 09:45", "2023-10-06 09:30", "2023-10-06 09:15"}
      };
      entries.add(new Object[]{
          "SIMPLE_DATE_FORMAT|yyyy-MM-dd HH:mm:ss|SECONDS|1", "SIMPLE_DATE_FORMAT|yyyy-MM-dd HH:mm|GMT|MINUTES|1",
          "MINUTES|15", "MINUTES|60", input, expected
      });
    }
    {
      // Test conversion from one simple date format to another with 1h window and 15m hop. Different timezone
      String[] input = {
          "2023-10-06 10:12:46"
      };
      String[][] expected = {
          {"2023-10-06 03:00", "2023-10-06 02:45", "2023-10-06 02:30", "2023-10-06 02:15"}
      };
      entries.add(new Object[]{
          "SIMPLE_DATE_FORMAT|yyyy-MM-dd HH:mm:ss|SECONDS|1", "SIMPLE_DATE_FORMAT|yyyy-MM-dd "
          + "HH:mm|America/Los_Angeles", "MINUTES|15", "MINUTES|60", input, expected
      });
    }

    return entries.toArray(new Object[entries.size()][]);
  }
}
