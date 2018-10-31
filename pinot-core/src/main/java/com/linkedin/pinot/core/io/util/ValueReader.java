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
package com.linkedin.pinot.core.io.util;

import java.io.IOException;


/**
 * Interface for value readers, which read a value at a given index.
 */
public interface ValueReader {
  int getInt(int index);

  long getLong(int index);

  float getFloat(int index);

  double getDouble(int index);

  String getUnpaddedString(int index, int numBytesPerValue, byte paddingByte, byte[] buffer);

  String getPaddedString(int index, int numBytesPerValue, byte[] buffer);

  byte[] getBytes(int index, int numBytesPerValue, byte[] buffer);

  void close() throws IOException;
}
