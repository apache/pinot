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
package org.apache.pinot.segment.local.upsert.merger;

/**
 * Merges 2 records and returns the merged record.
 * Append the new value from incoming row to the existing value from multi-value field. Then return the merged record.
 * Append merger allows duplicated records in the multi-value field.
 */
public class AppendMerger implements PartialUpsertMerger {
  AppendMerger() {
  }

  /**
   * Append the new value from incoming row to the given multi-value field of previous record.
   */
  @Override
  public Object merge(Object previousValue, Object currentValue) {
    return append((Object[]) previousValue, (Object[]) currentValue);
  }

  private static Object append(Object[] a, Object[] b) {
    Object[] merged = new Object[a.length + b.length];
    int count = 0;

    System.arraycopy(a, 0, merged, 0, a.length);
    System.arraycopy(b, 0, merged, a.length, b.length);
    return merged;
  }
}
