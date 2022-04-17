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

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import fmpp.Engine;
import fmpp.ProgressListener;
import fmpp.progresslisteners.TerseConsoleProgressListener;
import fmpp.setting.Settings;
import fmpp.util.MiscUtil;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;

import static java.lang.String.format;


/**
 * a maven plugin to run the freemarker generation incrementally
 * (if output has not changed, the files are not touched)
 *
 * @goal generate
 * @phase generate-sources
 */
public class FMPPMojo extends AbstractMojo {

  /**
   * Used to add new source directories to the build.
   *
   * @parameter default-value="${project}"
   * @required
   * @readonly
   **/
  private MavenProject project;

  /**
   * Where to find the FreeMarker template files.
   *
   * @parameter default-value="src/main/resources/fmpp/templates/"
   * @required
   */
  private File templates;

  /**
   * Where to write the generated files of the output files.
   *
   * @parameter default-value="${project.build.directory}/generated-sources/fmpp/"
   * @required
   */
  private File output;

  /**
   * Location of the FreeMarker config file.
   *
   * @parameter default-value="src/main/resources/fmpp/config.fmpp"
   * @required
   */
  private File config;

  /**
   * compilation scope to be added to ("compile" or "test")
   *
   * @parameter default-value="compile"
   * @required
   */
  private String scope;

  /**
   * FMPP data model build parameter.
   *
   * @see <a href="http://fmpp.sourceforge.net/settings.html#key_data">FMPP Data Model Building</a>
   * @parameter default-value=""
   */
  private String data;

  /**
   * if maven properties are added as data
   *
   * @parameter default-value="true"
   * @required
   */
  private boolean addMavenDataLoader;

  @Override
  public void execute()
      throws MojoExecutionException, MojoFailureException {
    if (project == null) {
      throw new MojoExecutionException("This plugin can only be used inside a project.");
    }
    String outputPath = output.getAbsolutePath();
    if ((!output.exists() && !output.mkdirs()) || !output.isDirectory()) {
      throw new MojoFailureException("can not write to output dir: " + outputPath);
    }
    String templatesPath = templates.getAbsolutePath();
    if (!templates.exists() || !templates.isDirectory()) {
      throw new MojoFailureException("templates not found in dir: " + outputPath);
    }

    // add the output directory path to the project source directories
    switch (scope) {
      case "compile":
        project.addCompileSourceRoot(outputPath);
        break;
      case "test":
        project.addTestCompileSourceRoot(outputPath);
        break;
      default:
        throw new MojoFailureException("scope must be compile or test");
    }

    final Stopwatch sw = Stopwatch.createStarted();
    try {
      getLog().info(
          format("Freemarker generation:\n scope: %s,\n config: %s,\n templates: %s", scope, config.getAbsolutePath(),
              templatesPath));
      final File tmp = Files.createTempDirectory("freemarker-tmp").toFile();
      String tmpPath = tmp.getAbsolutePath();
      final String tmpPathNormalized = tmpPath.endsWith(File.separator) ? tmpPath : tmpPath + File.separator;
      Settings settings = new Settings(new File("."));
      settings.set(Settings.NAME_SOURCE_ROOT, templatesPath);
      settings.set(Settings.NAME_OUTPUT_ROOT, tmp.getAbsolutePath());
      settings.load(config);
      settings.addProgressListener(new TerseConsoleProgressListener());
      settings.addProgressListener(new ProgressListener() {
        @Override
        public void notifyProgressEvent(Engine engine, int event, File src, int pMode, Throwable error, Object param)
            throws Exception {
          if (event == EVENT_END_PROCESSING_SESSION) {
            getLog().info(format("Freemarker generation took %dms", sw.elapsed(TimeUnit.MILLISECONDS)));
            sw.reset();
            Report report = moveIfChanged(tmp, tmpPathNormalized);
            if (!tmp.delete()) {
              throw new MojoFailureException(format("can not delete %s", tmp));
            }
            getLog().info(format("Incremental output update took %dms", sw.elapsed(TimeUnit.MILLISECONDS)));
            getLog().info(format("new: %d", report.newFiles));
            getLog().info(format("changed: %d", report.changedFiles));
            getLog().info(format("unchanged: %d", report.unchangedFiles));
          }
        }
      });
      List<String> dataValues = new ArrayList<>();
      if (addMavenDataLoader) {
        getLog().info("Adding maven data loader");
        settings.setEngineAttribute(MavenDataLoader.MAVEN_DATA_ATTRIBUTE, new MavenDataLoader.MavenData(project));
        dataValues.add(format("maven: %s()", MavenDataLoader.class.getName()));
      }
      if (data != null) {
        dataValues.add(data);
      }
      if (!dataValues.isEmpty()) {
        String dataString = Joiner.on(",").join(dataValues);
        getLog().info("Setting data loader " + dataString);

        settings.add(Settings.NAME_DATA, dataString);
      }
      settings.execute();
    } catch (Exception e) {
      throw new MojoFailureException(MiscUtil.causeMessages(e), e);
    }
  }

  private static final class Report {
    int changedFiles;
    int unchangedFiles;
    int newFiles;

    Report(int changedFiles, int unchangedFiles, int newFiles) {
      super();
      this.changedFiles = changedFiles;
      this.unchangedFiles = unchangedFiles;
      this.newFiles = newFiles;
    }

    public Report() {
      this(0, 0, 0);
    }

    void add(Report other) {
      changedFiles += other.changedFiles;
      unchangedFiles += other.unchangedFiles;
      newFiles += other.newFiles;
    }

    public void addChanged() {
      ++changedFiles;
    }

    public void addNew() {
      ++newFiles;
    }

    public void addUnchanged() {
      ++unchangedFiles;
    }
  }

  private Report moveIfChanged(File root, String tmpPath)
      throws MojoFailureException, IOException {
    Report report = new Report();
    for (File file : root.listFiles()) {
      if (file.isDirectory()) {
        report.add(moveIfChanged(file, tmpPath));
        if (!file.delete()) {
          throw new MojoFailureException(format("can not delete %s", file));
        }
      } else {
        String absPath = file.getAbsolutePath();
        if (!absPath.startsWith(tmpPath)) {
          throw new MojoFailureException(format("%s should start with %s", absPath, tmpPath));
        }
        String relPath = absPath.substring(tmpPath.length());
        File outputFile = new File(output, relPath);
        if (!outputFile.exists()) {
          report.addNew();
        } else if (!FileUtils.contentEquals(file, outputFile)) {
          getLog().info(format("%s has changed", relPath));
          if (!outputFile.delete()) {
            throw new MojoFailureException(format("can not delete %s", outputFile));
          }
          report.addChanged();
        } else {
          report.addUnchanged();
        }
        if (!outputFile.exists()) {
          File parentDir = outputFile.getParentFile();
          if (parentDir.exists() && !parentDir.isDirectory()) {
            throw new MojoFailureException(
                format("can not move %s to %s as %s is not a dir", file, outputFile, parentDir));
          }
          if (!parentDir.exists() && !parentDir.mkdirs()) {
            throw new MojoFailureException(
                format("can not move %s to %s as dir %s can not be created", file, outputFile, parentDir));
          }
          FileUtils.moveFile(file, outputFile);
        } else {
          if (!file.delete()) {
            throw new MojoFailureException(format("can not delete %s", file));
          }
        }
      }
    }
    return report;
  }
}
