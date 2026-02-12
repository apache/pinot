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

package org.apache.pinot.core.transport;

import java.lang.reflect.Constructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Represents an instance of Netty, allowing access to certain static properties via reflection, with support for
/// shaded Netty versions.
///
/// We know 2 common Netty instances:
/// - Unshaded Netty, which uses the standard `io.netty` package
/// - gRPC-shaded Netty, shaded by gRPC and included as a dependency. It uses `io.grpc.netty.shaded.io.netty` package.
///
/// This is important because Netty defines is not designed to be shaded, and it uses some static attributes to
/// determine its behavior, specially whether it can use `Unsafe` or not or how much memory to allocate for direct
/// buffers.
/// These attributes are set using JAVA_OPTs. Each shaded version uses different JAVA_OPT properties. If we forget to
/// set one of these properties for a shaded version, that Netty _instance_ will fall back to some default behavior that
/// may not be optimal, and we won't have any indication of that happening.
///
/// Given we do not shade Netty our-selves, we can access the different copies of the Netty classes without using
/// reflection. [NettyInstance] provides an abstraction to access the different Netty instances and their properties in
/// a simple way.
///
/// **It is critical to not shade this class**, otherwise the literals used for reflection
/// (ie `io.netty.util.internal.PlatformDependent`) will be shaded too, so instead of looking for
/// `io.grpc.netty.shaded.io.netty.util.internal.PlatformDependent`, as you may think reading this class, the shaded
/// version of this class will look for
/// `io.grpc.netty.shaded.org.apache.pinot.shaded.io.netty.util.internal.PlatformDependent`.
/// At the moment this is written, Pinot _does not_ shade Netty, so it is safe. Just to be sure, this class should be
/// excluded in the maven shade plugin configuration (see pom.xml on the root of the project).
public abstract class NettyInstance {
  private static final Constructor<DummyClass> CONSTRUCTOR;

  static {
    try {
      CONSTRUCTOR = DummyClass.class.getConstructor();
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("This should never happen, DummyClass has a default constructor", e);
    }
  }

  /// The name of the Netty instance. It will be used on logs but also on metric names, so it should be
  /// something short and without spaces like "Unshaded", "Pinot", "gRPC".
  /// Add underscores if you need to separate words, but avoid other special characters.
  public abstract String getName();

  public abstract String getShadePrefix();

  public abstract boolean isExplicitTryReflectionSetAccessible();

  public abstract long getUsedDirectMemory();

  public abstract long getMaxDirectMemory();

  public static class UnshadedNettyInstance extends NettyInstance {
    @Override
    public String getName() {
      return "unshaded";
    }

    @Override
    public String getShadePrefix() {
      return "";
    }

    @Override
    public boolean isExplicitTryReflectionSetAccessible() {
      return io.netty.util.internal.ReflectionUtil.trySetAccessible(CONSTRUCTOR, true) == null;
    }

    @Override
    public long getUsedDirectMemory() {
      return io.netty.util.internal.PlatformDependent.usedDirectMemory();
    }

    @Override
    public long getMaxDirectMemory() {
      return io.netty.util.internal.PlatformDependent.maxDirectMemory();
    }
  }

  public static class GrpcNettyInstance extends NettyInstance {
    @Override
    public String getName() {
      return "gRPC-shaded";
    }

    @Override
    public String getShadePrefix() {
      return "io.grpc.netty.shaded.";
    }

    @Override
    public boolean isExplicitTryReflectionSetAccessible() {
      return io.grpc.netty.shaded.io.netty.util.internal.ReflectionUtil.trySetAccessible(CONSTRUCTOR, true) == null;
    }

    @Override
    public long getUsedDirectMemory() {
      return io.grpc.netty.shaded.io.netty.util.internal.PlatformDependent.usedDirectMemory();
    }

    @Override
    public long getMaxDirectMemory() {
      return io.grpc.netty.shaded.io.netty.util.internal.PlatformDependent.maxDirectMemory();
    }
  }

  private static class DummyClass {
    public DummyClass() {
      // This does nothing, but it is used to test if we can set accessible to a class that is not related to Netty,
      // which is what we do in the isExplicitTryReflectionSetAccessible() method of NettyInspector.
    }
  }
}
