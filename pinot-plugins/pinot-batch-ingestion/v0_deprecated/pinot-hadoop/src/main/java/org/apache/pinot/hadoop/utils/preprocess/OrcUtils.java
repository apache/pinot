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
package org.apache.pinot.hadoop.utils.preprocess;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.mapred.OrcTimestamp;


public class OrcUtils {
  private OrcUtils() {
  }

  /**
   * Converts the ORC value into Number or String.
   * <p>The following ORC types are supported:
   * <ul>
   *   <li>IntWritable -> Integer</li>
   *   <li>LongWritable -> Long</li>
   *   <li>FloatWritable -> Float</li>
   *   <li>DoubleWritable -> Double</li>
   *   <li>Text -> String</li>
   *   <li>BooleanWritable -> String</li>
   *   <li>ByteWritable -> Byte</li>
   *   <li>ShortWritable -> Short</li>
   *   <li>DateWritable -> Long</li>
   *   <li>OrcTimestamp -> Long</li>
   * </ul>
   */
  public static Object convert(WritableComparable orcValue) {
    if (orcValue instanceof IntWritable) {
      return ((IntWritable) orcValue).get();
    }
    if (orcValue instanceof LongWritable) {
      return ((LongWritable) orcValue).get();
    }
    if (orcValue instanceof FloatWritable) {
      return ((FloatWritable) orcValue).get();
    }
    if (orcValue instanceof DoubleWritable) {
      return ((DoubleWritable) orcValue).get();
    }
    if (orcValue instanceof Text) {
      return orcValue.toString();
    }
    if (orcValue instanceof BooleanWritable) {
      return Boolean.toString(((BooleanWritable) orcValue).get());
    }
    if (orcValue instanceof ByteWritable) {
      return ((ByteWritable) orcValue).get();
    }
    if (orcValue instanceof ShortWritable) {
      return ((ShortWritable) orcValue).get();
    }
    if (orcValue instanceof DateWritable) {
      return ((DateWritable) orcValue).get().getTime();
    }
    if (orcValue instanceof OrcTimestamp) {
      return ((OrcTimestamp) orcValue).getTime();
    }
    throw new IllegalArgumentException(
        String.format("Illegal ORC value: %s, class: %s", orcValue, orcValue.getClass()));
  }
}
