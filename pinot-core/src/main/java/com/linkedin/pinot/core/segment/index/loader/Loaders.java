/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.index.loader;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.column.ColumnIndexContainer;
import com.linkedin.pinot.core.segment.index.converter.SegmentFormatConverter;
import com.linkedin.pinot.core.segment.index.converter.SegmentFormatConverterFactory;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import com.linkedin.pinot.core.segment.store.SegmentDirectoryPaths;
import com.linkedin.pinot.core.startree.StarTree;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Loaders {
  private static final Logger LOGGER = LoggerFactory.getLogger(Loaders.class);

  public static class IndexSegment {
    public static com.linkedin.pinot.core.indexsegment.IndexSegment load(File indexDir, ReadMode mode) throws Exception {
      return load(indexDir, mode, null);
    }

    public static com.linkedin.pinot.core.indexsegment.IndexSegment load(File indexDir, ReadMode readMode,
        IndexLoadingConfigMetadata indexLoadingConfigMetadata) throws Exception {

      Preconditions.checkNotNull(indexDir);
      Preconditions.checkArgument(indexDir.exists(), "Index directory: {} does not exist", indexDir);
      Preconditions.checkArgument(indexDir.isDirectory(), "Index directory: {} is not a directory", indexDir);
      //NOTE: indexLoadingConfigMetadata can be null

      SegmentMetadataImpl metadata = new SegmentMetadataImpl(indexDir);
      SegmentVersion configuredVersionToLoad = getSegmentVersionToLoad(indexLoadingConfigMetadata);
      SegmentVersion metadataVersion = metadata.getSegmentVersion();
      if (shouldConvertFormat(metadataVersion, configuredVersionToLoad) &&
          ! targetFormatAlreadyExists(indexDir, configuredVersionToLoad)) {
        LOGGER.info("segment:{} needs to be converted from :{} to {} version.", indexDir.getName(),
            metadataVersion, configuredVersionToLoad);
        SegmentFormatConverter converter = SegmentFormatConverterFactory.getConverter(metadataVersion, configuredVersionToLoad);
        LOGGER.info("Using converter:{} to up-convert the format", converter.getClass().getName());
        converter.convert(indexDir);
        LOGGER.info("Successfully up-converted segment:{} from :{} to {} version.",
            indexDir.getName(), metadataVersion, configuredVersionToLoad);
      }

      File segmentDirectoryPath = SegmentDirectoryPaths.segmentDirectoryFor(indexDir, configuredVersionToLoad);
      // load the metadata again since converter may have changed it
      metadata = new SegmentMetadataImpl(segmentDirectoryPath);

      // add or removes indexes based on indexLoadingConfigurationMetadata
      try (SegmentPreProcessor preProcessor = new SegmentPreProcessor(segmentDirectoryPath, metadata, indexLoadingConfigMetadata)) {
        preProcessor.process();
      }

      SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(segmentDirectoryPath, metadata, readMode);

      Map<String, ColumnIndexContainer> indexContainerMap = new HashMap<String, ColumnIndexContainer>();
      for (String column : metadata.getColumnMetadataMap().keySet()) {
        indexContainerMap.put(column, ColumnIndexContainer.init(segmentDirectory.createReader(),
            metadata.getColumnMetadataFor(column), indexLoadingConfigMetadata));
      }

      // The star tree index (if available)
      StarTree starTree = null;
      if (metadata.hasStarTree()) {
        File starTreeFile = new File(indexDir, V1Constants.STAR_TREE_INDEX_FILE);
        LOGGER.debug("Loading star tree index file {}", starTreeFile);
        starTree = StarTree.fromBytes(new FileInputStream(starTreeFile));
      }
      return new IndexSegmentImpl(segmentDirectory, metadata, indexContainerMap, starTree);
    }

    static boolean targetFormatAlreadyExists(File indexDir, SegmentVersion expectedSegmentVersion) {
      return SegmentDirectoryPaths.segmentDirectoryFor(indexDir, expectedSegmentVersion).exists();
    }

    static boolean shouldConvertFormat(SegmentVersion metadataVersion, SegmentVersion expectedSegmentVersion) {
      return (metadataVersion != expectedSegmentVersion);
    }

    private static SegmentVersion getSegmentVersionToLoad(IndexLoadingConfigMetadata indexLoadingConfigMetadata) {
      if (indexLoadingConfigMetadata == null) {
        return SegmentVersion.fromStringOrDefault(CommonConstants.Server.DEFAULT_SEGMENT_FORMAT_VERSION);
      }
      String versionName = indexLoadingConfigMetadata.segmentVersionToLoad();
      return SegmentVersion.fromStringOrDefault(versionName);
    }
  }
}
