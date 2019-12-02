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
package org.apache.pinot.tools.tuner.meta.manager.collector;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.tools.tuner.query.src.InputIterator;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The iterator over all tarred segments in _directory/tableName/
 */
public class CompressedFilePathIter implements InputIterator {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompressedFilePathIter.class);
  private Iterator<Pair<String, File>> _iterator;

  private String _segmentTarDir;

  private CompressedFilePathIter(Builder builder) {
    _segmentTarDir = builder._directory;
  }

  private CompressedFilePathIter openDirectory()
      throws FileNotFoundException {
    File dir = new File(_segmentTarDir);
    if (!dir.exists() || dir.isFile()) {
      LOGGER.error("Wrong input directory!");
      throw new FileNotFoundException();
    }

    ArrayList<Pair<String, File>> validTableNameWithoutTypeSegmentFile = new ArrayList<>();

    Arrays.stream(Objects.requireNonNull(dir.listFiles())).filter(
        tableDir -> (tableDir.isDirectory() && !tableDir.getName().startsWith(".") && !tableDir.getName()
            .equals("Deleted_Segments"))).forEach(
        tableDir -> Arrays.stream(Objects.requireNonNull(tableDir.listFiles()))
            .filter(file -> (file.isFile() && !file.getName().startsWith(".")))
            .forEach(file -> validTableNameWithoutTypeSegmentFile.add(Pair.of(tableDir.getName(), file))));

    _iterator = validTableNameWithoutTypeSegmentFile.iterator();
    return this;
  }

  @Override
  public boolean hasNext() {
    return _iterator.hasNext();
  }

  @Override
  public AbstractQueryStats next()
      throws NoSuchElementException {
    Pair<String, File> nextTuple = _iterator.next();
    return new PathWrapper.Builder().setTableNameWithoutType(nextTuple.getLeft()).setFile(nextTuple.getRight()).build();
  }

  @Override
  public void close() {
  }

  public static final class Builder {
    private String _directory;

    public Builder() {
    }

    public Builder setDirectory(String val) {
      _directory = val;
      return this;
    }

    public CompressedFilePathIter build()
        throws FileNotFoundException {
      return new CompressedFilePathIter(this).openDirectory();
    }
  }
}
