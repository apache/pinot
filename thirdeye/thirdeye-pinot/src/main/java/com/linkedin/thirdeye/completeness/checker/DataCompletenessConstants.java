/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.completeness.checker;

import java.util.concurrent.TimeUnit;

/**
 * Constants needed for data completeness checker
 */
public class DataCompletenessConstants {

  public enum DataCompletenessType {
    CHECKER,
    CLEANUP
  }

  public static int LOOKBACK_TIME_DURATION = 3;
  public static TimeUnit LOOKBACK_TIMEUNIT = TimeUnit.DAYS;

  // A little over a month is needed for the basic wo4w function
  public static int CLEANUP_TIME_DURATION = 40;
  public static TimeUnit CLEANUP_TIMEUNIT = TimeUnit.DAYS;

}
