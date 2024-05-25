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
package org.apache.pinot.core.query.aggregation.function.funnel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;


public class FunnelStepEvent implements Comparable<FunnelStepEvent> {
  public final static int SIZE_IN_BYTES = Long.BYTES + Integer.BYTES;

  private final long _timestamp;
  private final int _step;

  public FunnelStepEvent(long timestamp, int step) {
    _timestamp = timestamp;
    _step = step;
  }

  public FunnelStepEvent(byte[] bytes) {
    try (DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes))) {
      _timestamp = dataInputStream.readLong();
      _step = dataInputStream.readInt();
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while converting byte[] to FunnelStepEvent", e);
    }
  }

  public long getTimestamp() {
    return _timestamp;
  }

  public int getStep() {
    return _step;
  }

  @Override
  public String toString() {
    return "StepEvent{" + "timestamp=" + _timestamp + ", step=" + _step + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FunnelStepEvent stepEvent = (FunnelStepEvent) o;

    if (_timestamp != stepEvent._timestamp) {
      return false;
    }
    return _step == stepEvent._step;
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(_timestamp);
    result = 31 * result + _step;
    return result;
  }

  @Override
  public int compareTo(FunnelStepEvent o) {
    if (_timestamp < o._timestamp) {
      return -1;
    } else if (_timestamp > o._timestamp) {
      return 1;
    } else {
      return Integer.compare(_step, o._step);
    }
  }

  public byte[] getBytes() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      dataOutputStream.writeLong(_timestamp);
      dataOutputStream.writeInt(_step);
      dataOutputStream.close();
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while converting FunnelStepEvent to byte[]", e);
    }
    return byteArrayOutputStream.toByteArray();
  }
}
