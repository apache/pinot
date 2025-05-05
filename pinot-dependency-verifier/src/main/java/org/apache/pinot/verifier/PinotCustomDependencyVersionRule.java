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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Named;
import org.apache.maven.enforcer.rule.api.EnforcerRule;
import org.apache.maven.enforcer.rule.api.EnforcerRuleException;
import org.apache.maven.enforcer.rule.api.EnforcerRuleHelper;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.model.Dependency;
import org.apache.maven.model.DependencyManagement;
import org.apache.maven.model.Model;
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
  private List<String> _skipModuleList;

//  public void setSkipRoot(boolean skipRoot) {
//    _skipRoot = skipRoot;
//  }

  public void setSkipModules(String skipModules) {
    _skipModules = skipModules;
    _skipModuleList = parseSkipModules(skipModules);
  }

  @Override
  public void execute(final EnforcerRuleHelper helper) throws EnforcerRuleException {
    final MavenProject project;
    final MavenSession session;
    try {
      project = (MavenProject) helper.evaluate("${project}");
      session = (MavenSession) helper.evaluate("${session}");
    } catch (Exception e) {
      throw new EnforcerRuleException("Unable to retrieve MavenProject", e);
    }

    Model originalModel = project.getOriginalModel();

    // Check if Root POM has hardcoded in <dependencyManagement>
    if (project.isExecutionRoot()) {
      DependencyManagement depMgmt = originalModel.getDependencyManagement();
      if (depMgmt == null || depMgmt.getDependencies() == null) {
        return;
      }

      for (Dependency dep : depMgmt.getDependencies()) {
        String version = dep.getVersion();
        if (version != null && !version.trim().startsWith("${")) {
          throw new EnforcerRuleException(String.format(
              "Root POM has hardcoded version '%s' in <dependencyManagement> for %s:%s. Use a property instead.",
              dep.getVersion(), dep.getGroupId(), dep.getArtifactId()
          ));
        }
      }
      return;
    }

    // Skip configured modules
    if (_skipModules != null && !_skipModules.trim().isEmpty()) {
      Path rootPath = session.getTopLevelProject().getBasedir().toPath().toAbsolutePath().normalize();
      Path modulePath = project.getBasedir().toPath().toAbsolutePath().normalize();
      String pathString = modulePath.toString();
      for (String skip : _skipModuleList) {
        if (pathString.contains(skip)) {
          return;
        }
      }
    }

    // Any dependencies listed under <dependencies> in any submodule POM should not include <version> tags
    List<Dependency> deps = originalModel.getDependencies();
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

  private List<String> parseSkipModules(String skips) {
    if (skips == null || skips.isBlank()) {
      return new ArrayList<>();
    }
    return Arrays.stream(_skipModules.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }
}
