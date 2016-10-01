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
package com.linkedin.pinot.core.segment.index.loader;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.column.ColumnIndexContainer;
import com.linkedin.pinot.core.segment.index.converter.SegmentFormatConverter;
import com.linkedin.pinot.core.segment.index.converter.SegmentFormatConverterFactory;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import com.linkedin.pinot.core.segment.store.SegmentDirectoryPaths;
import com.linkedin.pinot.core.startree.StarTreeFormatVersion;
import com.linkedin.pinot.core.startree.StarTreeInterf;
import com.linkedin.pinot.core.startree.StarTreeSerDe;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Loaders {
  private static final Logger LOGGER = LoggerFactory.getLogger(Loaders.class);

  public static class IndexSegment {
    public static com.linkedin.pinot.core.indexsegment.IndexSegment load(File indexDir, ReadMode readMode)
        throws Exception {
      return load(indexDir, readMode, null, null);
    }

    public static com.linkedin.pinot.core.indexsegment.IndexSegment load(File indexDir, ReadMode readMode,
        IndexLoadingConfigMetadata indexLoadingConfigMetadata)
        throws Exception {
      return load(indexDir, readMode, indexLoadingConfigMetadata, null);
    }

    public static com.linkedin.pinot.core.indexsegment.IndexSegment load(File indexDir, ReadMode readMode,
        IndexLoadingConfigMetadata indexLoadingConfigMetadata, Schema schema)
        throws Exception {
      Preconditions.checkNotNull(indexDir);
      Preconditions.checkArgument(indexDir.exists(), "Index directory: {} does not exist", indexDir);
      Preconditions.checkArgument(indexDir.isDirectory(), "Index directory: {} is not a directory", indexDir);
      // NOTE: indexLoadingConfigMetadata and schema can be null.


      if (indexLoadingConfigMetadata != null) {
        StarTreeFormatVersion starTreeVersionToLoad = getStarTreeVersionToLoad(indexLoadingConfigMetadata);
        StarTreeSerDe.convertStarTreeFormatIfNeeded(indexDir, starTreeVersionToLoad);
      }


      SegmentVersion configuredVersionToLoad = getSegmentVersionToLoad(indexLoadingConfigMetadata);
      if (!targetFormatAlreadyExists(indexDir, configuredVersionToLoad)) {
        SegmentMetadataImpl metadata = new SegmentMetadataImpl(indexDir);
        SegmentVersion metadataVersion = metadata.getSegmentVersion();
        if (shouldConvertFormat(metadataVersion, configuredVersionToLoad)) {
          LOGGER.info("segment:{} needs to be converted from :{} to {} version.", indexDir.getName(), metadataVersion,
              configuredVersionToLoad);
          SegmentFormatConverter converter =
              SegmentFormatConverterFactory.getConverter(metadataVersion, configuredVersionToLoad);
          LOGGER.info("Using converter:{} to up-convert segment: {}", converter.getClass().getName(), indexDir.getName());
          converter.convert(indexDir);
          LOGGER
              .info("Successfully up-converted segment:{} from :{} to {} version.", indexDir.getName(), metadataVersion,
                  configuredVersionToLoad);
        }
      }
      // add or removes indexes based on indexLoadingConfigurationMetadata
      // add or replace new columns with default value
      // NOTE: this step may modify the segment metadata.
      File segmentDirectoryPath = SegmentDirectoryPaths.segmentDirectoryFor(indexDir, configuredVersionToLoad);
      try (SegmentPreProcessor preProcessor = new SegmentPreProcessor(segmentDirectoryPath, indexLoadingConfigMetadata,
          schema)) {
        preProcessor.process();
      }

      // load the metadata again since converter and pre-processor may have changed it
      SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDirectoryPath);
      SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(segmentDirectoryPath, metadata, readMode);

      Map<String, ColumnIndexContainer> indexContainerMap = new HashMap<String, ColumnIndexContainer>();
      SegmentDirectory.Reader segmentReader = segmentDirectory.createReader();
      for (String column : metadata.getColumnMetadataMap().keySet()) {
        indexContainerMap.put(column, ColumnIndexContainer.init(segmentReader,
            metadata.getColumnMetadataFor(column), indexLoadingConfigMetadata));
      }

      // load star tree index if it exists
      StarTreeInterf starTree = null;
      if (segmentReader.hasStarTree()) {
        LOGGER.debug("Loading star tree for segment: {}", segmentDirectory);
        starTree = StarTreeSerDe.fromFile(segmentReader.getStarTreeFile(), readMode);
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

    /**
     * Helper method to determine the star tree format version to load.
     * Determines the version based on the index loading config.
     * If the config is null, returns the default value as specified in
     * {@link CommonConstants.Server#DEFAULT_STAR_TREE_FORMAT_VERSION}.
     *
     * @param indexLoadingConfigMetadata Index loading config
     * @return Star Tree format version to load
     */
    private static StarTreeFormatVersion getStarTreeVersionToLoad(
        IndexLoadingConfigMetadata indexLoadingConfigMetadata) {
      String starTreeFormatVersionString = CommonConstants.Server.DEFAULT_STAR_TREE_FORMAT_VERSION;

      if (indexLoadingConfigMetadata != null) {
        starTreeFormatVersionString = indexLoadingConfigMetadata.getStarTreeVersionToLoad();
      }
      return StarTreeFormatVersion.valueOf(starTreeFormatVersionString);
    }
  }
}
