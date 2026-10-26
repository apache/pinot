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


/// What a client knew about a failure that its exception does not say: whose credential it used, and any status or
/// code it is about to discard (a synthesized "topic does not exist", a cause it drops).
///
/// [#attach] adds it to the failure as a suppressed exception, so the thrown type never changes and the evidence
/// travels with every wrapper that keeps the cause. [IngestionSourceErrorClassifier] reads it as evidence for the
/// failure it is attached to and the causes beneath, and never classifies it as a failure of its own.
///
/// It holds only bounded values: enums, a status, a code token, and a resource reduced to scheme, host and port. It
/// shows up in stack traces and logs, so it must never hold message text, user info or query strings.
public final class IngestionSourceErrorEvidence extends RuntimeException {
  private static final long serialVersionUID = 1L;

  /// The code for a source that never answered (DNS, connect, TLS, timeout), for a client that drops the cause that
  /// would show it.
  public static final String UNREACHABLE = "unreachable";
  /// The code for a table credential a client's own check rejected before any request: it can't be parsed, or has
  /// expired.
  public static final String LOCAL_PRECHECK = "local_precheck";
  /// The code for Pinot's own setup failing before any request reached the source, e.g. a trust store it can't load.
  public static final String LOCAL_SETUP = "local_setup";
  /// The code a Kafka client records when it synthesizes "topic does not exist" from a metadata answer that lacks the
  /// topic: Kafka's own `UNKNOWN_TOPIC_OR_PARTITION` error, which the synthesized exception doesn't carry.
  public static final String UNKNOWN_TOPIC_OR_PARTITION = "UNKNOWN_TOPIC_OR_PARTITION";

  // Throwables built with suppression disabled can't carry evidence; their evidence lives here, keyed by identity.
  private static final ConcurrentMap<Throwable, IngestionSourceErrorEvidence> UNSUPPRESSIBLE =
      new MapMaker().weakKeys().makeMap();

  @Nullable
  private String _target;
  private int _status;
  @Nullable
  private String _code;
  @Nullable
  private IngestionSourceCredentialMode _credentialMode;
  private boolean _roleArnConfigured;
  @Nullable
  private String _resource;
  private boolean _customerConfigured = true;
  private boolean _boundary;

  public IngestionSourceErrorEvidence() {
    super(null, null, false, false);
  }

  /// Attaches a copy of `evidence` to `failure` and returns `failure`. A failure that already carries evidence keeps
  /// it: the innermost client is the closest to the source.
  ///
  /// Attach only to an exception created for this failure, such as the plugin's own wrapper. Some clients throw
  /// shared instances (Kafka's `Errors.X.exception()`), and evidence attached to one would apply to every later
  /// failure that reuses it.
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

  /// The evidence attached directly to `failure`, or null. Evidence on its causes is not considered.
  @Nullable
  public static IngestionSourceErrorEvidence of(Throwable failure) {
    for (Throwable suppressed : failure.getSuppressed()) {
      if (suppressed instanceof IngestionSourceErrorEvidence) {
        return (IngestionSourceErrorEvidence) suppressed;
      }
    }
    return UNSUPPRESSIBLE.get(failure);
  }

  /// The evidence of a failure that couldn't carry it as a suppressed exception, or null.
  @Nullable
  static IngestionSourceErrorEvidence ofUnsuppressible(Throwable failure) {
    return UNSUPPRESSIBLE.get(failure);
  }

  /// The service the client was talking to when it isn't the source itself, e.g.
  /// [IngestionSourceErrorVerdict#TARGET_SCHEMA_REGISTRY].
  @Nullable
  public String getTarget() {
    return _target;
  }

  public IngestionSourceErrorEvidence setTarget(@Nullable String target) {
    _target = IngestionSourceErrorVerdict.token(target);
    return this;
  }

  /// The status the transport saw, or 0 when none was recorded.
  public int getStatus() {
    return _status;
  }

  /// The source's own error code the transport saw, or one of the codes defined here, or null.
  @Nullable
  public String getCode() {
    return _code;
  }

  /// Records evidence the transport is about to discard. Only bounded values belong here, never message text.
  public IngestionSourceErrorEvidence setEvidence(int status, @Nullable String code) {
    _status = status;
    _code = IngestionSourceErrorVerdict.token(code);
    return this;
  }

  @Nullable
  public IngestionSourceCredentialMode getCredentialMode() {
    return _credentialMode;
  }

  public IngestionSourceErrorEvidence setCredentialMode(@Nullable IngestionSourceCredentialMode credentialMode) {
    _credentialMode = credentialMode;
    return this;
  }

  /// Whether the client assumes a role on top of its base credential. Only its presence is recorded, never the role.
  public boolean isRoleArnConfigured() {
    return _roleArnConfigured;
  }

  public IngestionSourceErrorEvidence setRoleArnConfigured(boolean roleArnConfigured) {
    _roleArnConfigured = roleArnConfigured;
    return this;
  }

  /// The endpoint as the client resolved it, reduced to scheme, host and port, or null. The built-in rules don't read
  /// it; registered rules and log lines can.
  @Nullable
  public String getResource() {
    return _resource;
  }

  /// Records the endpoint the client resolved. User info, path and query, which can carry credentials or
  /// signatures, are dropped; a value that isn't a URI with a host is recorded as `unknown`.
  public IngestionSourceErrorEvidence setResource(@Nullable String uri) {
    _resource = resourceOf(uri);
    return this;
  }

  /// Whether the client talks to a source the table configured, rather than one Pinot itself was configured with
  /// (a batch reader can fall back to Pinot's deep store). Stream clients always talk to the table's source. The
  /// built-in rules don't read it; registered rules can.
  public boolean isCustomerConfigured() {
    return _customerConfigured;
  }

  public IngestionSourceErrorEvidence setCustomerConfigured(boolean customerConfigured) {
    _customerConfigured = customerConfigured;
    return this;
  }

  /// Whether the failure this evidence is attached to marks a call into the source; see [#setBoundary].
  public boolean isBoundary() {
    return _boundary;
  }

  /// Marks the failure this evidence is attached to as a call into the source. When any exception of a failure carries
  /// boundary evidence, only the exceptions at or beneath a boundary in their own chain are read as the source's, and a
  /// suppressed exception counts only through a boundary of its own. A caller whose one operation also reads storage
  /// that isn't the source, such as Pinot's own deep store, marks each source call this way. A stream client, whose
  /// whole failure comes from the source, doesn't need to.
  public IngestionSourceErrorEvidence setBoundary(boolean boundary) {
    _boundary = boundary;
    return this;
  }

  /// A copy to attach to one failure: a stack trace prints a throwable it meets twice as a circular reference.
  private IngestionSourceErrorEvidence copy() {
    IngestionSourceErrorEvidence copy = new IngestionSourceErrorEvidence().setTarget(_target)
        .setEvidence(_status, _code).setCredentialMode(_credentialMode).setRoleArnConfigured(_roleArnConfigured)
        .setCustomerConfigured(_customerConfigured).setBoundary(_boundary);
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
    if (_credentialMode != null) {
      sb.append(" credentialMode=").append(_credentialMode);
    }
    if (_roleArnConfigured) {
      sb.append(" roleArnConfigured");
    }
    if (_resource != null) {
      sb.append(" resource=").append(_resource);
    }
    if (!_customerConfigured) {
      sb.append(" notCustomerConfigured");
    }
    if (_boundary) {
      sb.append(" boundary");
    }
    return sb.toString();
  }
}
