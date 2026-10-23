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
package org.apache.pinot.spi.plugin;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;


/// Test utility classloader that defines its own copy of one class from the parent's class-file bytes instead of
/// delegating, producing a `Class` object with the same name but a different defining classloader. Everything else
/// (including the class's supertypes and any service interface) is delegated to the parent so the copy remains a
/// valid service provider.
///
/// Used by `ServiceLoader` discovery tests (here and in downstream modules via the pinot-spi test-jar) to simulate
/// a classloader shipping its own version-skewed copy of a provider class.
public class ChildFirstClassLoader extends URLClassLoader {
  private final String _childFirstClassName;

  public ChildFirstClassLoader(URL[] urls, ClassLoader parent, String childFirstClassName) {
    super(urls, parent);
    _childFirstClassName = childFirstClassName;
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve)
      throws ClassNotFoundException {
    if (!name.equals(_childFirstClassName)) {
      return super.loadClass(name, resolve);
    }
    synchronized (getClassLoadingLock(name)) {
      Class<?> loaded = findLoadedClass(name);
      if (loaded == null) {
        try (InputStream inputStream = getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
          if (inputStream == null) {
            throw new ClassNotFoundException(name);
          }
          byte[] classBytes = inputStream.readAllBytes();
          loaded = defineClass(name, classBytes, 0, classBytes.length);
        } catch (IOException e) {
          throw new ClassNotFoundException(name, e);
        }
      }
      if (resolve) {
        resolveClass(loaded);
      }
      return loaded;
    }
  }
}
