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
package org.apache.pinot.fmpp;

import fmpp.Engine;
import fmpp.tdd.DataLoader;
import java.util.List;
import org.apache.maven.project.MavenProject;


/**
 * A data loader for Maven
 */
public class MavenDataLoader implements DataLoader {
  public static final class MavenData {
    private final MavenProject project;

    public MavenData(MavenProject project) {
      this.project = project;
    }

    public MavenProject getProject() {
      return project;
    }
  }

  public static final String MAVEN_DATA_ATTRIBUTE = "maven.data";

  @Override
  public Object load(Engine e, List args)
      throws Exception {
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("maven model data loader has no parameters");
    }

    MavenData data = (MavenData) e.getAttribute(MAVEN_DATA_ATTRIBUTE);
    return data;
  }
}
