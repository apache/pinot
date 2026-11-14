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
package org.apache.pinot.spi.ingestion.error;

import com.google.common.collect.MapMaker;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;


public final class IngestionSourceErrorEvidence extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public static final String UNREACHABLE = "unreachable";
  public static final String LOCAL_PRECHECK = "local_precheck";
  public static final String UNKNOWN_TOPIC_OR_PARTITION = "UNKNOWN_TOPIC_OR_PARTITION";

  private static final ConcurrentMap<Throwable, IngestionSourceErrorEvidence> UNSUPPRESSIBLE =
      new MapMaker().weakKeys().makeMap();

  @Nullable
  private String _target;
  private int _status;
  @Nullable
  private String _code;
  @Nullable
  private String _resource;
  private boolean _boundary;

  public IngestionSourceErrorEvidence() {
    super(null, null, false, false);
  }

  public static <T extends Throwable> T attach(T failure, IngestionSourceErrorEvidence evidence) {
    if (of(failure) != null) {
      return failure;
    }
    IngestionSourceErrorEvidence copy = evidence.copy();
    failure.addSuppressed(copy);
    if (of(failure) == null) {
      UNSUPPRESSIBLE.put(failure, copy);
    }
    return failure;
  }

  @Nullable
  public static IngestionSourceErrorEvidence of(Throwable failure) {
    for (Throwable suppressed : failure.getSuppressed()) {
      if (suppressed instanceof IngestionSourceErrorEvidence) {
        return (IngestionSourceErrorEvidence) suppressed;
      }
    }
    return UNSUPPRESSIBLE.get(failure);
  }

  @Nullable
  static IngestionSourceErrorEvidence ofUnsuppressible(Throwable failure) {
    return UNSUPPRESSIBLE.get(failure);
  }

  @Nullable
  public String getTarget() {
    return _target;
  }

  public IngestionSourceErrorEvidence setTarget(@Nullable String target) {
    _target = IngestionSourceErrorVerdict.token(target);
    return this;
  }

  public int getStatus() {
    return _status;
  }

  @Nullable
  public String getCode() {
    return _code;
  }

  public IngestionSourceErrorEvidence setEvidence(int status, @Nullable String code) {
    _status = status;
    _code = IngestionSourceErrorVerdict.token(code);
    return this;
  }

  @Nullable
  public String getResource() {
    return _resource;
  }

  public IngestionSourceErrorEvidence setResource(@Nullable String uri) {
    _resource = resourceOf(uri);
    return this;
  }

  public boolean isBoundary() {
    return _boundary;
  }

  public IngestionSourceErrorEvidence setBoundary(boolean boundary) {
    _boundary = boundary;
    return this;
  }

  private IngestionSourceErrorEvidence copy() {
    IngestionSourceErrorEvidence copy = new IngestionSourceErrorEvidence().setTarget(_target)
        .setEvidence(_status, _code).setBoundary(_boundary);
    copy._resource = _resource;
    return copy;
  }

  @Nullable
  private static String resourceOf(@Nullable String uri) {
    if (uri == null) {
      return null;
    }
    try {
      URI parsed = new URI(uri.trim());
      if (parsed.getScheme() == null || parsed.getHost() == null) {
        return "unknown";
      }
      return parsed.getScheme() + "://" + parsed.getHost() + (parsed.getPort() >= 0 ? ":" + parsed.getPort() : "");
    } catch (URISyntaxException e) {
      return "unknown";
    }
  }

  @Override
  public String getMessage() {
    StringBuilder sb = new StringBuilder("Ingestion source evidence:");
    if (_target != null) {
      sb.append(" target=").append(_target);
    }
    if (_status != 0) {
      sb.append(" status=").append(_status);
    }
    if (_code != null) {
      sb.append(" code=").append(_code);
    }
    if (_resource != null) {
      sb.append(" resource=").append(_resource);
    }
    if (_boundary) {
      sb.append(" boundary");
    }
    return sb.toString();
  }
}
