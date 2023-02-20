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

package org.apache.pinot.segment.spi.index;

import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;


// This should be a sealed class in modern Java
public class IndexDeclaration<C> {
  private static final IndexDeclaration<Object> DECLARED_DISABLED = new IndexDeclaration<>(true, null);
  private static final IndexDeclaration<Object> DEFAULT_DISABLED = new IndexDeclaration<>(false, null);
  private final boolean _declared;
  private final C _config;

  private IndexDeclaration() {
    _declared = false;
    _config = null;
  }

  IndexDeclaration(boolean declared, @Nullable C config) {
    _declared = declared;
    _config = config;
  }

  public static <C> IndexDeclaration<C> notDeclared(IndexType<C, ?, ?> indexType) {
    C defaultConfig = indexType.getDefaultConfig();
    if (defaultConfig != null) {
      return new IndexDeclaration<>(false, defaultConfig);
    } else {
      return (IndexDeclaration<C>) DEFAULT_DISABLED;
    }
  }

  public static <C> IndexDeclaration<C> notDeclared(C defaultConfig) {
    return new IndexDeclaration<>(false, defaultConfig);
  }

  public static <C> IndexDeclaration<C> notDeclaredDisabled() {
    return (IndexDeclaration<C>) DEFAULT_DISABLED;
  }

  public static <C> IndexDeclaration<C> declaredDisabled() {
    return (IndexDeclaration<C>) DECLARED_DISABLED;
  }

  public static <C> IndexDeclaration<C> declared(@Nullable C config) {
    if (config == null) {
      return declaredDisabled();
    }
    return new IndexDeclaration<>(true, config);
  }

  public boolean isEnabled() {
    return _config != null;
  }

  public boolean isDeclared() {
    return _declared;
  }

  public C getEnabledConfig() {
    Preconditions.checkState(isDeclared() || isEnabled(), "This index is either not declared or not enabled");
    return _config;
  }

  /**
   * Returns the configuration associated with this declaration or null when it is disabled.
   * <ol>
   *   <li>When the declaration is explicit, then the explicit config is returned</li>
   *   <li>When the declaration is by default, then the default config is returned, which may be null</li>
   * </ol>
   * @see #getEnabledConfig()
   */
  @Nullable
  public C getConfigOrNull() {
    return _config;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (!(o instanceof IndexDeclaration)) {
      return false;
    }
    IndexDeclaration<?> that = (IndexDeclaration<?>) o;
    return _declared == that._declared && Objects.equals(_config, that._config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_declared, _config);
  }

  @Override
  public String toString() {
    return "IndexDeclaration{" + "_declared=" + _declared + ", _config=" + _config + '}';
  }
}
