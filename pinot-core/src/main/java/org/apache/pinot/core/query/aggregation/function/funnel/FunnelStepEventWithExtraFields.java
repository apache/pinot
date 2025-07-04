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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class FunnelStepEventWithExtraFields implements Comparable<FunnelStepEventWithExtraFields> {
  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final FunnelStepEvent _funnelStepEvent;
  private final List<Object> _extraFields;

  public FunnelStepEventWithExtraFields(FunnelStepEvent funnelStepEvent, List<Object> extraFields) {
    _funnelStepEvent = funnelStepEvent;
    _extraFields = extraFields;
  }

  public FunnelStepEventWithExtraFields(byte[] bytes) {
    _funnelStepEvent = new FunnelStepEvent(Arrays.copyOf(bytes, FunnelStepEvent.SIZE_IN_BYTES));
    try {
      _extraFields = OBJECT_MAPPER.readValue(bytes, 2, bytes.length, new TypeReference<List<Object>>() {
      });
    } catch (IOException e) {
      throw new RuntimeException("Caught exception while converting byte[] to FunnelStepEventWithExtraFields", e);
    }
  }

  public FunnelStepEvent getFunnelStepEvent() {
    return _funnelStepEvent;
  }

  public List<Object> getExtraFields() {
    return _extraFields;
  }

  @Override
  public String toString() {
    return "StepEventWithExtraFields{" + "funnelStepEvent=" + _funnelStepEvent + ", extraFields=" + _extraFields + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FunnelStepEventWithExtraFields stepEvent = (FunnelStepEventWithExtraFields) o;

    if (!_funnelStepEvent.equals(stepEvent.getFunnelStepEvent())) {
      return false;
    }
    return _extraFields.equals(stepEvent.getExtraFields());
  }

  @Override
  public int hashCode() {
    int result = _funnelStepEvent.hashCode();
    result = 31 * result + _extraFields.hashCode();
    return result;
  }

  @Override
  public int compareTo(FunnelStepEventWithExtraFields o) {
    return _funnelStepEvent.compareTo(o.getFunnelStepEvent());
  }

  public byte[] getBytes() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      dataOutputStream.write(_funnelStepEvent.getBytes());
      dataOutputStream.write(OBJECT_MAPPER.writeValueAsBytes(_extraFields));
      dataOutputStream.close();
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while converting FunnelStepEvent to byte[]", e);
    }
    return byteArrayOutputStream.toByteArray();
  }
}
