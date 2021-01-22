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
package org.apache.pinot.minion.executor;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.minion.PinotTaskConfig;


/**
 * The class <code>SegmentGenerationAndPushResult</code> wraps the segment generation and push
 * results.
 */
public class SegmentGenerationAndPushResult {
  private final boolean _succeed;
  private final String _segmentName;
  private final Exception _exception;
  private final Map<String, Object> _customProperties;

  private SegmentGenerationAndPushResult(boolean succeed, String segmentName, Exception exception,
      Map<String, Object> customProperties) {
    _succeed = succeed;
    _segmentName = segmentName;
    _exception = exception;
    _customProperties = customProperties;
  }

  @SuppressWarnings("unchecked")
  public <T> T getCustomProperty(String key) {
    return (T) _customProperties.get(key);
  }

  public Exception getException() {
    return _exception;
  }

  public boolean isSucceed() {
    return _succeed;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public static class Builder {
    private boolean _succeed;
    private String _segmentName;
    private Exception _exception;
    private final Map<String, Object> _customProperties = new HashMap<>();

    public Builder setSucceed(boolean succeed) {
      _succeed = succeed;
      return this;
    }

    public void setSegmentName(String segmentName) {
      _segmentName = segmentName;
    }

    public Builder setException(Exception exception) {
      _exception = exception;
      return this;
    }

    public Builder setCustomProperty(String key, Object property) {
      _customProperties.put(key, property);
      return this;
    }

    public SegmentGenerationAndPushResult build() {
      return new SegmentGenerationAndPushResult(_succeed, _segmentName, _exception, _customProperties);
    }
  }
}
