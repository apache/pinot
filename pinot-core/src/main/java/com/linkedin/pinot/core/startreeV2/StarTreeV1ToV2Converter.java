/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.startreeV2;

import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;


public class StarTreeV1ToV2Converter implements StarTreeFormatConverter {

  private static final String V3_TEMP_DIR_SUFFIX = ".v3.tmp";


  @Override
  public void convert(File indexStarTreeDir, int starTreeId) throws Exception {
    File v3TempDirectory = v3ConversionTempDirectory(indexStarTreeDir);
    setDirectoryPermissions(v3TempDirectory);

    return;
  }

  public File v3ConversionTempDirectory(File v2SegmentDirectory) throws IOException {
    File v3TempDirectory = Files.createTempDirectory(v2SegmentDirectory.toPath(), v2SegmentDirectory.getName() + V3_TEMP_DIR_SUFFIX).toFile();

    return v3TempDirectory;
  }

  private void setDirectoryPermissions(File v3Directory)
      throws IOException {
    EnumSet<PosixFilePermission> permissions = EnumSet
        .of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE,
            PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE,
            PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_EXECUTE);

    Files.setPosixFilePermissions(v3Directory.toPath(), permissions);
  }
}
