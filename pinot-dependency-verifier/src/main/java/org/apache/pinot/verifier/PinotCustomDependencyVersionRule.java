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
package org.apache.pinot.verifier;
import java.util.List;
import javax.inject.Named;
import org.apache.maven.enforcer.rule.api.EnforcerRule;
import org.apache.maven.enforcer.rule.api.EnforcerRuleException;
import org.apache.maven.enforcer.rule.api.EnforcerRuleHelper;
import org.apache.maven.model.Dependency;
import org.apache.maven.project.MavenProject;

/**
 * Enforces that no submodule declares a hardcoded <version> on its <dependency> entries.
 * Versions must be managed centrally in the root POM's <dependencyManagement>.
 */
@Named("pinotCustomDependencyVersionRule")
public class PinotCustomDependencyVersionRule implements EnforcerRule {

  /**
   * If true, skip this rule in the root project (where versions belong).
   */
  private boolean _skipRoot = true;

  /**
   * Comma-separated list of artifactIds to skip (e.g. "pinot-plugins,pinot-connectors").
   */
  private String _skipModules;

  public void setSkipRoot(boolean skipRoot) {
    _skipRoot = skipRoot;
  }

  public void setSkipModules(String skipModules) {
    _skipModules = skipModules;
  }

  @Override
  public void execute(final EnforcerRuleHelper helper) throws EnforcerRuleException {
    final MavenProject project;
    try {
      project = (MavenProject) helper.evaluate("${project}");
    } catch (Exception e) {
      throw new EnforcerRuleException("Unable to retrieve MavenProject", e);
    }

    // Skip root if configured
    if (_skipRoot && project.isExecutionRoot()) {
      return;
    }

    // Skip configured modules
    if (_skipModules != null) {
      for (String skip : _skipModules.split(",")) {
        if (project.getArtifactId().equals(skip.trim())) {
          return;
        }
      }
    }

    List<Dependency> deps = project.getDependencies();
    for (Dependency d : deps) {
      if (d.getVersion() != null) {
        throw new EnforcerRuleException(
            String.format("Module '%s' declares version '%s' for dependency %s:%s. "
                    + "Versions must be managed in root dependencyManagement.",
                project.getArtifactId(), d.getVersion(), d.getGroupId(), d.getArtifactId())
        );
      }
    }
  }

  @Override
  public String getCacheId() {
    // Include skipRoot and skipModules in cache key
    return String.format("skipRoot=%s;skipModules=%s", _skipRoot, _skipModules);
  }
  @Override
  public boolean isCacheable() {
    return true;
  }

  @Override
  public boolean isResultValid(EnforcerRule arg0) {
    return false;
  }
}
