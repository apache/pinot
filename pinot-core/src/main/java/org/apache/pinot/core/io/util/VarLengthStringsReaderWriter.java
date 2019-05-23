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
package org.apache.pinot.core.io.util;

import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;

public class VarLengthStringsReaderWriter extends VarLengthBytesValueReaderWriter {

  private static byte[][] convertToByteArrays(String[] strings) {
    byte[][] byteArrays = new byte[strings.length][];
    for (int i = 0; i < strings.length; i++) {
      byteArrays[i] = StringUtil.encodeUtf8(strings[i]);
    }
    return byteArrays;
  }

  public static long getRequiredSize(String[] strings) {
    return getRequiredSize(convertToByteArrays(strings));
  }

  public VarLengthStringsReaderWriter(PinotDataBuffer dataBuffer) {
    super(dataBuffer);
  }

  public void init(String[] strings) {
    super.init(convertToByteArrays(strings));
  }

  @Override
  public String getString(int index) {
    return StringUtil.decodeUtf8(getBytes(index));
  }
}
