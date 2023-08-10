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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


@Test(suiteName = "CustomClusterIntegrationTest")
public class ThetaSketchTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "ThetaSketchTest";
  private static final String DIM_NAME = "dimName";
  private static final String DIM_VALUE = "dimValue";
  private static final String SHARD_ID = "shardId";
  private static final String THETA_SKETCH = "thetaSketchCol";

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(DIM_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DIM_VALUE, FieldSpec.DataType.STRING)
        .addSingleValueDimension(SHARD_ID, FieldSpec.DataType.INT)
        .addSingleValueDimension(THETA_SKETCH, FieldSpec.DataType.BYTES).build();
  }

  @Override
  protected long getCountStarResult() {
    /*
    Uploaded table content:

    row#  dimName  dimValue  shardId  thetaSketchCol
    ----  =======  ========  =======  ==============
    1     Course   Math      1        ...
    2     Course   History   1        ...
    3     Course   Biology   1        ...
    4     Gender   Female    1        ...
    5     Gender   Male      1        ...
    6     Course   Math      2        ...
    7     Course   History   2        ...
    8     Course   Biology   2        ...
    9     Gender   Female    2        ...
    10    Gender   Male      2        ...
     */
    return 10;
  }

  @Override
  public File createAvroFile()
      throws IOException {

    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(new org.apache.avro.Schema.Field(DIM_NAME, org.apache.avro.Schema.create(
            org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(DIM_VALUE, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(SHARD_ID, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null,
            null),
        new org.apache.avro.Schema.Field(THETA_SKETCH, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES),
            null, null)));

    // create avro file
    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);

      int studentId = 0;
      int cardinality = 50;
      for (int shardId = 0; shardId < 2; shardId++) {

        // populate student-course data (studentId, gender, course) for this shard id
        String[] allGenders = {"Female", "Male"};
        String[] allCountries = {"Math", "History", "Biology"};
        Map<Pair<String, String>, List<Integer>> genderCourseToStudentIds = new HashMap<>();
        for (String gender : allGenders) {
          for (String course : allCountries) {
            List<Integer> studentIds =
                genderCourseToStudentIds.computeIfAbsent(ImmutablePair.of(gender, course), key -> new ArrayList<>());
            for (int i = 0; i < cardinality; i++) {
              studentIds.add(studentId++);
            }
            cardinality += 10;
          }
        }

        // [gender dimension] calculate theta sketches & add them to avro file
        for (String gender : allGenders) {

          // calculate theta sketch
          UpdateSketch sketch = new UpdateSketchBuilder().build();
          genderCourseToStudentIds.forEach((genderCourse, studentIds) -> {
            if (gender.equals(genderCourse.getLeft())) {
              studentIds.forEach(sketch::update);
            }
          });

          // create avro record
          GenericData.Record record = new GenericData.Record(avroSchema);
          record.put(DIM_NAME, "gender");
          record.put(DIM_VALUE, gender);
          record.put(SHARD_ID, shardId);
          record.put(THETA_SKETCH, ByteBuffer.wrap(sketch.compact().toByteArray()));

          // add avro record to file
          fileWriter.append(record);
        }

        // [course dimension] calculate theta sketches & add them to avro file
        for (String course : allCountries) {

          // calculate theta sketch
          UpdateSketch sketch = new UpdateSketchBuilder().build();
          genderCourseToStudentIds.forEach((genderCourse, studentIds) -> {
            if (course.equals(genderCourse.getRight())) {
              studentIds.forEach(sketch::update);
            }
          });

          // create avro record
          GenericData.Record record = new GenericData.Record(avroSchema);
          record.put(DIM_NAME, "course");
          record.put(DIM_VALUE, course);
          record.put(SHARD_ID, shardId);
          record.put(THETA_SKETCH, ByteBuffer.wrap(sketch.compact().toByteArray()));

          // add avro record to file
          fileWriter.append(record);
        }
      }
    }

    return avroFile;
  }

  @Test(dataProvider = "useV1QueryEngine")
  public void testThetaSketchQueryV1(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    /*
    Original data:

    Gender    Course   Shard#1  Shard#2
    --------  -------  -------  -------
    Female    Math     50       110
    Female    History  60       120
    Female    Biology  70       130
    Male      Math     80       140
    Male      History  90       150
    Male      Biology  100      160
     */

    // gender = female
    {
      String query = "select distinctCountThetaSketch(thetaSketchCol) from " + getTableName()
          + " where dimName = 'gender' and dimValue = 'Female'";
      int expected = 50 + 60 + 70 + 110 + 120 + 130;
      runAndAssert(query, expected);

      query = "select distinctCountThetaSketch(thetaSketchCol, '', 'dimName = ''gender'' and dimValue = ''Female''', "
          + "'$1') from " + getTableName();
      runAndAssert(query, expected);

      query = "select distinctCountThetaSketch(thetaSketchCol, '', "
          + "'dimName = ''gender''', 'dimValue = ''Female''', 'SET_INTERSECT($1, $2)') from " + getTableName();
      runAndAssert(query, expected);
    }

    // gender = male
    {
      String query = "select distinctCountThetaSketch(thetaSketchCol) from " + getTableName()
          + " where dimName = 'gender' and dimValue = 'Male'";
      int expected = 80 + 90 + 100 + 140 + 150 + 160;
      runAndAssert(query, expected);

      query =
          "select distinctCountThetaSketch(thetaSketchCol, '', 'dimName = ''gender'' and dimValue = ''Male''', '$1') "
              + "from " + getTableName();
      runAndAssert(query, expected);

      query = "select distinctCountThetaSketch(thetaSketchCol, '', "
          + "'dimName = ''gender''', 'dimValue = ''Male''', 'SET_INTERSECT($1, $2)') from " + getTableName();
      runAndAssert(query, expected);
    }

    // course = math
    {
      String query = "select distinctCountThetaSketch(thetaSketchCol) from " + getTableName()
          + " where dimName = 'course' AND dimValue = 'Math'";
      int expected = 50 + 80 + 110 + 140;
      runAndAssert(query, expected);

      query =
          "select distinctCountThetaSketch(thetaSketchCol, '', 'dimName = ''course'' and dimValue = ''Math''', '$1') "
              + "from " + getTableName();
      runAndAssert(query, expected);

      query = "select distinctCountThetaSketch(thetaSketchCol, '', "
          + "'dimName = ''course''', 'dimValue = ''Math''', 'SET_INTERSECT($1, $2)') from " + getTableName();
      runAndAssert(query, expected);
    }

    // gender = female INTERSECT course = math
    {
      String query = "select distinctCountThetaSketch(thetaSketchCol, '', "
          + "'dimName = ''gender'' and dimValue = ''Female''', 'dimName = ''course'' and dimValue = ''Math''', "
          + "'SET_INTERSECT($1, $2)') from " + getTableName();
      int expected = 50 + 110;
      runAndAssert(query, expected);

      query = "select distinctCountThetaSketch(thetaSketchCol, '', "
          + "'dimName = ''gender''', 'dimValue = ''Female''', 'dimName = ''course''', 'dimValue = ''Math''', "
          + "'SET_INTERSECT($1, $2, $3, $4)') from " + getTableName();
      runAndAssert(query, expected);

      query = "select distinctCountThetaSketch(thetaSketchCol, '', "
          + "'dimName = ''gender''', 'dimValue = ''Female''', 'dimName = ''course''', 'dimValue = ''Math''', "
          + "'SET_INTERSECT(SET_INTERSECT($1, $2), SET_INTERSECT($3, $4))') from " + getTableName();
      runAndAssert(query, expected);
    }

    // gender = male UNION course = biology
    {
      String query = "select distinctCountThetaSketch(thetaSketchCol, '', "
          + "'dimName = ''gender'' and dimValue = ''Male''', 'dimName = ''course'' and dimValue = ''Biology''', "
          + "'SET_UNION($1, $2)') from " + getTableName();
      int expected = 70 + 80 + 90 + 100 + 130 + 140 + 150 + 160;
      runAndAssert(query, expected);

      query = "select distinctCountThetaSketch(thetaSketchCol, '', "
          + "'dimName = ''gender''', 'dimValue = ''Male''', 'dimName = ''course''', 'dimValue = ''Biology''', "
          + "'SET_UNION(SET_INTERSECT($1, $2), SET_INTERSECT($3, $4))') from " + getTableName();
      runAndAssert(query, expected);
    }

    // gender = female DIFF course = history
    {
      String query = "select distinctCountThetaSketch(thetaSketchCol, '', "
          + "'dimName = ''gender'' and dimValue = ''Female''', 'dimName = ''course'' and dimValue = ''History''', "
          + "'SET_DIFF($1, $2)') from " + getTableName();
      int expected = 50 + 110 + 70 + 130;
      runAndAssert(query, expected);

      query = "select distinctCountThetaSketch(thetaSketchCol, '', "
          + "'dimName = ''gender''', 'dimValue = ''Female''', 'dimName = ''course''', 'dimValue = ''History''', "
          + "'SET_DIFF(SET_INTERSECT($1, $2), SET_INTERSECT($3, $4))') from " + getTableName();
      runAndAssert(query, expected);
    }

    // group by gender
    {
      String query = "select dimValue, distinctCountThetaSketch(thetaSketchCol) from " + getTableName()
          + " where dimName = 'gender' group by dimValue";
      ImmutableMap<String, Integer> expected =
          ImmutableMap.of("Female", 50 + 60 + 70 + 110 + 120 + 130, "Male", 80 + 90 + 100 + 140 + 150 + 160);
      runAndAssert(query, expected);
    }
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testThetaSketchQueryV2(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    /*
    Original data:

    Gender    Course   Shard#1  Shard#2
    --------  -------  -------  -------
    Female    Math     50       110
    Female    History  60       120
    Female    Biology  70       130
    Male      Math     80       140
    Male      History  90       150
    Male      Biology  100      160
     */

    // gender = female
    {
      String query = "select distinctCountThetaSketch(thetaSketchCol) from " + getTableName()
          + " where dimName = 'gender' and dimValue = 'Female'";
      int expected = 50 + 60 + 70 + 110 + 120 + 130;
      runAndAssert(query, expected);

      query = "select getThetaSketchEstimate(distinctCountRAWThetaSketch(thetaSketchCol)"
          + " FILTER (WHERE dimName = 'gender' and dimValue = 'Female')) from " + getTableName();
      runAndAssert(query, expected);

      query = "select GET_THETA_SKETCH_ESTIMATE(THETA_SKETCH_INTERSECT( "
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimName = 'gender'),"
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimValue = 'Female'))) "
          + "  FROM " + getTableName();
      runAndAssert(query, expected);
    }

    // gender = male
    {
      String query = "select distinctCountThetaSketch(thetaSketchCol) from " + getTableName()
          + " where dimName = 'gender' and dimValue = 'Male'";
      int expected = 80 + 90 + 100 + 140 + 150 + 160;
      runAndAssert(query, expected);

      query = "select getThetaSketchEstimate(distinctCountRAWThetaSketch(thetaSketchCol)"
          + " FILTER (WHERE dimName = 'gender' and dimValue = 'Male')) from " + getTableName();
      runAndAssert(query, expected);

      query = "select GET_THETA_SKETCH_ESTIMATE(THETA_SKETCH_INTERSECT( "
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimName = 'gender'),"
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimValue = 'Male'))) "
          + "  FROM " + getTableName();
      runAndAssert(query, expected);
    }

    // course = math
    {
      String query = "select distinctCountThetaSketch(thetaSketchCol) from " + getTableName()
          + " where dimName = 'course' AND dimValue = 'Math'";
      int expected = 50 + 80 + 110 + 140;
      runAndAssert(query, expected);

      query = "select getThetaSketchEstimate(distinctCountRAWThetaSketch(thetaSketchCol)"
          + " FILTER (WHERE dimName = 'course' and dimValue = 'Math')) from " + getTableName();
      runAndAssert(query, expected);

      query = "select GET_THETA_SKETCH_ESTIMATE(THETA_SKETCH_INTERSECT( "
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimName = 'course'),"
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimValue = 'Math'))) "
          + "  FROM " + getTableName();
      runAndAssert(query, expected);
    }

    // gender = female INTERSECT course = math
    {
      String query = "select GET_THETA_SKETCH_ESTIMATE(THETA_SKETCH_INTERSECT( "
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER ("
          + "        WHERE dimName = 'gender' and dimValue = 'Female'), "
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER ("
          + "        WHERE dimName = 'course' and dimValue = 'Math'))) "
          + "  FROM " + getTableName();
      int expected = 50 + 110;
      runAndAssert(query, expected);

      query = "select GET_THETA_SKETCH_ESTIMATE(THETA_SKETCH_INTERSECT( "
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimName = 'gender'), "
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimValue = 'Female'), "
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimName = 'course'), "
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimValue = 'Math'))) "
          + "  FROM " + getTableName();
      runAndAssert(query, expected);

      query = "select GET_THETA_SKETCH_ESTIMATE(THETA_SKETCH_INTERSECT(THETA_SKETCH_INTERSECT("
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimName = 'gender'), "
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimValue = 'Female')), "
          + "  THETA_SKETCH_INTERSECT("
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimName = 'course'), "
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimValue = 'Math')))) "
          + "  FROM " + getTableName();
      runAndAssert(query, expected);
    }

    // gender = male UNION course = biology
    {
      String query = "select GET_THETA_SKETCH_ESTIMATE(THETA_SKETCH_UNION( "
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER ("
          + "        WHERE dimName = 'gender' and dimValue = 'Male'), "
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER ("
          + "        WHERE dimName = 'course' and dimValue = 'Biology'))) "
          + "  FROM " + getTableName();
      int expected = 70 + 80 + 90 + 100 + 130 + 140 + 150 + 160;
      runAndAssert(query, expected);

      query = "select GET_THETA_SKETCH_ESTIMATE(THETA_SKETCH_UNION("
          + "  THETA_SKETCH_INTERSECT("
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimName = 'gender'), "
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimValue = 'Male')), "
          + "  THETA_SKETCH_INTERSECT("
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimName = 'course'), "
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimValue = 'Biology')))) "
          + "  FROM " + getTableName();
      runAndAssert(query, expected);
    }

    // gender = female DIFF course = history
    {
      String query = "select GET_THETA_SKETCH_ESTIMATE(THETA_SKETCH_DIFF( "
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER ("
          + "        WHERE dimName = 'gender' and dimValue = 'Female'), "
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER ("
          + "        WHERE dimName = 'course' and dimValue = 'History'))) "
          + "  FROM " + getTableName();
      int expected = 50 + 110 + 70 + 130;
      runAndAssert(query, expected);

      query = "select GET_THETA_SKETCH_ESTIMATE(THETA_SKETCH_DIFF("
          + "  THETA_SKETCH_INTERSECT("
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimName = 'gender'), "
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimValue = 'Female')), "
          + "  THETA_SKETCH_INTERSECT("
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimName = 'course'), "
          + "    DISTINCT_COUNT_RAW_THETA_SKETCH(thetaSketchCol, '') FILTER (WHERE dimValue = 'History')))) "
          + "  FROM " + getTableName();
      runAndAssert(query, expected);
    }

    // group by gender
    {
      String query = "select dimValue, distinctCountThetaSketch(thetaSketchCol) from " + getTableName()
          + " where dimName = 'gender' group by dimValue";
      ImmutableMap<String, Integer> expected =
          ImmutableMap.of("Female", 50 + 60 + 70 + 110 + 120 + 130, "Male", 80 + 90 + 100 + 140 + 150 + 160);
      runAndAssert(query, expected);
    }
  }

  private void runAndAssert(String query, int expected)
      throws Exception {
    JsonNode jsonNode = postQuery(query);
    int actual = Integer.parseInt(jsonNode.get("resultTable").get("rows").get(0).get(0).asText());
    assertEquals(actual, expected);
  }

  private void runAndAssert(String query, Map<String, Integer> expectedGroupToValueMap)
      throws Exception {
    Map<String, Integer> actualGroupToValueMap = new HashMap<>();
    JsonNode jsonNode = postQuery(query);
    jsonNode.get("resultTable").get("rows").forEach(node -> {
      String group = node.get(0).textValue();
      int value = node.get(1).intValue();
      actualGroupToValueMap.put(group, value);
    });
    assertEquals(actualGroupToValueMap, expectedGroupToValueMap);
  }
}
