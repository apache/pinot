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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.pinot.spi.utils.StringUtils;


/**
 * Override the Text comparison logic to compare with the decoded String instead of the byte array.
 */
public class TextComparator extends WritableComparator {
  public TextComparator() {
    super(Text.class);
  }

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    int n1 = WritableUtils.decodeVIntSize(b1[s1]);
    int n2 = WritableUtils.decodeVIntSize(b2[s2]);
    return StringUtils.decodeUtf8(b1, s1 + n1, l1 - n1).compareTo(StringUtils.decodeUtf8(b2, s2 + n2, l2 - n2));
  }
}
