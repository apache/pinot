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
package org.apache.pinot.spark.jobs.preprocess;

import com.google.common.collect.Ordering;
import java.io.Serializable;


public class SparkDataPreprocessingComparator extends Ordering<Object> implements Serializable {
  @Override
  public int compare(Object left, Object right) {
    Object value1 = ((SparkDataPreprocessingJobKey) left).getSortedColumn();
    Object value2 = ((SparkDataPreprocessingJobKey) right).getSortedColumn();
    if (value1 == null) {
      return 0;
    }
    if (value1 instanceof Integer) {
      return Integer.compare((int) value1, (int) value2);
    } else if (value1 instanceof Long) {
      return Long.compare((long) value1, (long) value2);
    } else if (value1 instanceof Float) {
      return Float.compare((float) value1, (float) value2);
    } else if (value1 instanceof Double) {
      return Double.compare((double) value1, (double) value2);
    } else if (value1 instanceof Short) {
      return Short.compare((short) value1, (short) value2);
    } else if (value1 instanceof String) {
      return ((String) value1).compareTo((String) value2);
    }
    throw new RuntimeException("Unsupported Data type: " + value1.getClass().getName());
  }
}
