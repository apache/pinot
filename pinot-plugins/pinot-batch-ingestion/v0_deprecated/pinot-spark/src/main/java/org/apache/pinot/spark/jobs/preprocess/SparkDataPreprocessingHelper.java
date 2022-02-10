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
package org.apache.pinot.spark.jobs.preprocess;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.ingestion.preprocess.DataPreprocessingHelper;
import org.apache.pinot.ingestion.preprocess.partitioners.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;


public abstract class SparkDataPreprocessingHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(SparkDataPreprocessingHelper.class);

  protected DataPreprocessingHelper _dataPreprocessingHelper;

  public SparkDataPreprocessingHelper(DataPreprocessingHelper dataPreprocessingHelper) {
    _dataPreprocessingHelper = dataPreprocessingHelper;
  }

  public void registerConfigs(TableConfig tableConfig, Schema tableSchema, String partitionColumn, int numPartitions,
      String partitionFunction, String partitionColumnDefaultNullValue, String sortingColumn,
      FieldSpec.DataType sortingColumnType, String sortingColumnDefaultNullValue, int numOutputFiles,
      int maxNumRecordsPerFile) {
    _dataPreprocessingHelper
        .registerConfigs(tableConfig, tableSchema, partitionColumn, numPartitions, partitionFunction,
            partitionColumnDefaultNullValue, sortingColumn, sortingColumnType, sortingColumnDefaultNullValue,
            numOutputFiles, maxNumRecordsPerFile);
  }

  public void setUpAndExecuteJob(SparkSession sparkSession) {
    // Read data into data frame.
    Dataset<Row> dataFrame = sparkSession.read().format(getDataFormat())
        .load(convertPathsToStrings(_dataPreprocessingHelper._inputDataPaths));
    JavaRDD<Row> javaRDD = dataFrame.javaRDD();

    // Find positions of partition column and sorting column if specified.
    StructType schema = dataFrame.schema();
    StructField[] fields = schema.fields();
    int partitionColumnPosition = -1;
    int sortingColumnPosition = -1;
    PartitionFunction partitionFunction = null;
    String partitionColumnDefaultNullValue = null;
    for (int i = 0; i <= fields.length; i++) {
      StructField field = fields[i];
      if (_dataPreprocessingHelper._partitionColumn != null && _dataPreprocessingHelper._partitionColumn
          .equalsIgnoreCase(field.name())) {
        partitionColumnPosition = i;
        partitionFunction = PartitionFunctionFactory
            .getPartitionFunction(_dataPreprocessingHelper._partitionFunction, _dataPreprocessingHelper._numPartitions);
        partitionColumnDefaultNullValue =
            _dataPreprocessingHelper._pinotTableSchema.getFieldSpecFor(_dataPreprocessingHelper._partitionColumn)
                .getDefaultNullValueString();
      }
      if (_dataPreprocessingHelper._sortingColumn != null && _dataPreprocessingHelper._sortingColumn
          .equalsIgnoreCase(field.name())) {
        sortingColumnPosition = i;
      }
    }
    int numPartitions;
    if (partitionColumnPosition == -1) {
      if (_dataPreprocessingHelper._numOutputFiles > 0) {
        numPartitions = _dataPreprocessingHelper._numOutputFiles;
      } else {
        numPartitions = javaRDD.getNumPartitions();
      }
    } else {
      numPartitions = _dataPreprocessingHelper._numPartitions;
    }
    final String finalPartitionColumn = _dataPreprocessingHelper._partitionColumn;
    final int finalNumPartitions = numPartitions;
    final int finalPartitionColumnPosition = partitionColumnPosition;
    final int finalSortingColumnPosition = sortingColumnPosition;
    LOGGER.info("Partition column: " + finalPartitionColumn);
    LOGGER.info("Number of partitions: " + finalNumPartitions);
    LOGGER.info("Position of partition column (if specified): " + finalPartitionColumnPosition);
    LOGGER.info("Position of sorting column (if specified): " + finalSortingColumnPosition);
    LOGGER.info("Default null value for partition column: " + partitionColumnDefaultNullValue);
    SparkDataPreprocessingPartitioner sparkPartitioner =
        new SparkDataPreprocessingPartitioner(finalPartitionColumn, finalNumPartitions, partitionFunction,
            partitionColumnDefaultNullValue);

    // Convert to java pair rdd.
    JavaPairRDD<Object, Row> pairRDD = javaRDD.mapToPair((PairFunction<Row, Object, Row>) row -> {
      Object partitionColumnValue = null;
      Object sortingColumnValue = null;

      if (_dataPreprocessingHelper._partitionColumn != null) {
        partitionColumnValue = row.get(finalPartitionColumnPosition);
      }
      int partitionId = sparkPartitioner.generatePartitionId(partitionColumnValue);
      if (_dataPreprocessingHelper._sortingColumn != null) {
        sortingColumnValue = row.get(finalSortingColumnPosition);
      }
      return new Tuple2<>(new SparkDataPreprocessingJobKey(partitionId, sortingColumnValue), row);
    });

    // Repartition and sort within partitions.
    Comparator<Object> comparator = new SparkDataPreprocessingComparator();
    JavaPairRDD<Object, Row> partitionedSortedPairRDD =
        pairRDD.repartitionAndSortWithinPartitions(sparkPartitioner, comparator);

    // TODO: support preprocessing.max.num.records.per.file before writing back to storage
    // Write to output path.
    partitionedSortedPairRDD.values().saveAsTextFile(_dataPreprocessingHelper._outputPath.toString());
  }

  private Seq<String> convertPathsToStrings(List<Path> paths) {
    List<String> stringList = new ArrayList<>();
    for (Path path : paths) {
      stringList.add(path.toString());
    }
    return toSeq(stringList);
  }

  /**
   * Helper to wrap a Java collection into a Scala Seq
   *
   * @param collection java collection
   * @param <T> collection item type
   * @return Scala Seq of type T
   */
  public static <T> Seq<T> toSeq(Collection<T> collection) {
    return JavaConverters.asScalaBufferConverter(new ArrayList<>(collection)).asScala().toSeq();
  }

  public abstract String getDataFormat();
}
