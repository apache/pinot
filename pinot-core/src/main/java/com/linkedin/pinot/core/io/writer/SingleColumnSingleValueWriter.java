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
package com.linkedin.pinot.core.io.writer;

public interface SingleColumnSingleValueWriter extends DataFileWriter {
  /**
   *
   * @param row
   * @param ch
   */
  public void setChar(int row, char ch);

  /**
   *
   * @param row
   * @param i
   */
  public void setInt(int row, int i);

  /**
   *
   * @param row
   * @param s
   */
  public void setShort(int row, short s);

  /**
   *
   * @param row
   * @param l
   */
  public void setLong(int row, long l);

  /**
   *
   * @param row
   * @param f
   */
  public void setFloat(int row, float f);

  /**
   *
   * @param row
   * @param d
   */
  public void setDouble(int row, double d);

  /**
   *
   * @param row
   * @param string
   */
  public void setString(int row, String string);

  /**
   *
   * @param row
   * @param bytes
   */
  public void setBytes(int row, byte[] bytes);

}
