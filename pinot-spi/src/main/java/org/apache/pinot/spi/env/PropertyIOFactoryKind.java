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
package org.apache.pinot.spi.env;

import org.apache.commons.configuration2.PropertiesConfiguration;

public enum PropertyIOFactoryKind {
  ConfigFileIOFactory {
    public String toString() {
      return "ConfigFile";
    }

    @Override
    public ConfigFilePropertyIOFactory getInstance() {
      return new ConfigFilePropertyIOFactory();
    }
  },
  VersionedIOFactory {
    public String toString() {
      return "Versioned";
    }
    @Override
    public VersionedIOFactory getInstance() {
      return new VersionedIOFactory();
    }
  },
  DefaultIOFactory {
    public String toString() {
      return "Default";
    }
    @Override
    public PropertiesConfiguration.DefaultIOFactory getInstance() {
      return new PropertiesConfiguration.DefaultIOFactory();
    }
  };

  // get the instance of the IO factory.
  public abstract PropertiesConfiguration.DefaultIOFactory getInstance();
}
