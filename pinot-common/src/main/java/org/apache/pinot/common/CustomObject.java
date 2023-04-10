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

package org.apache.pinot.common;

import java.nio.ByteBuffer;


public class CustomObject {
  public static final int NULL_TYPE_VALUE = 100;

  private final int _type;
  private final ByteBuffer _buffer;

  public CustomObject(int type, ByteBuffer buffer) {
    _type = type;
    _buffer = buffer;
  }

  public int getType() {
    return _type;
  }

  public ByteBuffer getBuffer() {
    return _buffer;
  }
}
