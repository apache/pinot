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
package org.apache.pinot.spi.data;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Integration tests for enhanced schema compatibility checking with detailed error messages
 */
public class SchemaCompatibilityTest {

  @Test
  public void testBackwardCompatibleSchemas() {
    // Create old schema
    Schema oldSchema = new Schema.SchemaBuilder()
        .setSchemaName("test_table")
        .addSingleValueDimension("user_id", FieldSpec.DataType.STRING)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .addMetric("age", FieldSpec.DataType.INT)
        .addDateTime("created_at", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    // Create new compatible schema (add new columns)
    Schema newSchema = new Schema.SchemaBuilder()
        .setSchemaName("test_table")
        .addSingleValueDimension("user_id", FieldSpec.DataType.STRING)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .addSingleValueDimension("email", FieldSpec.DataType.STRING) // New column added
        .addMetric("age", FieldSpec.DataType.INT)
        .addMetric("score", FieldSpec.DataType.DOUBLE) // New metric added
        .addDateTime("created_at", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    // Test backward compatibility
    Assert.assertTrue(newSchema.isBackwardCompatibleWith(oldSchema));

    SchemaCompatibilityResult result = newSchema.checkBackwardCompatibilityWithDetails(oldSchema);
    Assert.assertTrue(result.isCompatible());
    Assert.assertTrue(result.getIssues().isEmpty());
    Assert.assertEquals(result.getDetailedErrorMessage(), "Schema is backward compatible");
    Assert.assertEquals(result.getSummaryErrorMessage(), "Schema is backward compatible");
  }

  @Test
  public void testMissingColumnIncompatibility() {
    // Create old schema
    Schema oldSchema = new Schema.SchemaBuilder()
        .setSchemaName("test_table")
        .addSingleValueDimension("user_id", FieldSpec.DataType.STRING)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .addMetric("age", FieldSpec.DataType.INT)
        .addDateTime("created_at", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    // Create new schema missing some columns
    Schema newSchema = new Schema.SchemaBuilder()
        .setSchemaName("test_table")
        .addSingleValueDimension("user_id", FieldSpec.DataType.STRING)
        .addMetric("age", FieldSpec.DataType.INT)
        // Missing: name, created_at
        .build();

    // Test backward compatibility
    Assert.assertFalse(newSchema.isBackwardCompatibleWith(oldSchema));

    SchemaCompatibilityResult result = newSchema.checkBackwardCompatibilityWithDetails(oldSchema);
    Assert.assertFalse(result.isCompatible());
    Assert.assertEquals(result.getIssues().size(), 2);

    // Check detailed error message contains missing columns
    String detailedMessage = result.getDetailedErrorMessage();
    Assert.assertTrue(detailedMessage.contains("Found 2 issue(s)"));
    Assert.assertTrue(detailedMessage.contains("name") && detailedMessage.contains("missing"));
    Assert.assertTrue(detailedMessage.contains("created_at") && detailedMessage.contains("missing"));
    Assert.assertTrue(detailedMessage.contains("💡 Suggestion: Add the missing column back"));

    // Check summary error message
    String summaryMessage = result.getSummaryErrorMessage();
    Assert.assertTrue(summaryMessage.contains("missing columns"));
    Assert.assertTrue(summaryMessage.contains("name") && summaryMessage.contains("created_at"));
  }

  @Test
  public void testDataTypeMismatchIncompatibility() {
    // Create old schema
    Schema oldSchema = new Schema.SchemaBuilder()
        .setSchemaName("test_table")
        .addSingleValueDimension("user_id", FieldSpec.DataType.STRING)
        .addMetric("age", FieldSpec.DataType.INT)
        .addMetric("score", FieldSpec.DataType.FLOAT)
        .build();

    // Create new schema with data type changes
    Schema newSchema = new Schema.SchemaBuilder()
        .setSchemaName("test_table")
        .addSingleValueDimension("user_id", FieldSpec.DataType.STRING)
        .addMetric("age", FieldSpec.DataType.LONG) // Changed INT to LONG
        .addMetric("score", FieldSpec.DataType.DOUBLE) // Changed FLOAT to DOUBLE
        .build();

    // Test backward compatibility
    Assert.assertFalse(newSchema.isBackwardCompatibleWith(oldSchema));

    SchemaCompatibilityResult result = newSchema.checkBackwardCompatibilityWithDetails(oldSchema);
    Assert.assertFalse(result.isCompatible());
    Assert.assertEquals(result.getIssues().size(), 2);

    // Check detailed error message
    String detailedMessage = result.getDetailedErrorMessage();
    Assert.assertTrue(detailedMessage.contains("data type mismatch"));
    Assert.assertTrue(detailedMessage.contains("age") && detailedMessage.contains("INT")
        && detailedMessage.contains("LONG"));
    Assert.assertTrue(detailedMessage.contains("score") && detailedMessage.contains("FLOAT")
        && detailedMessage.contains("DOUBLE"));
    Assert.assertTrue(detailedMessage.contains("💡 Suggestion"));

    // Check summary error message
    String summaryMessage = result.getSummaryErrorMessage();
    Assert.assertTrue(summaryMessage.contains("data type mismatches [age, score]"));
  }

  @Test
  public void testFieldTypeMismatchIncompatibility() {
    // Create old schema
    Schema oldSchema = new Schema.SchemaBuilder()
        .setSchemaName("test_table")
        .addSingleValueDimension("user_id", FieldSpec.DataType.STRING)
        .addSingleValueDimension("category", FieldSpec.DataType.STRING)
        .addMetric("count", FieldSpec.DataType.INT)
        .build();

    // Create new schema with field type changes
    Schema newSchema = new Schema.SchemaBuilder()
        .setSchemaName("test_table")
        .addSingleValueDimension("user_id", FieldSpec.DataType.STRING)
        .addMetric("category", FieldSpec.DataType.STRING) // Changed DIMENSION to METRIC (invalid but for testing)
        .addSingleValueDimension("count", FieldSpec.DataType.INT) // Changed METRIC to DIMENSION
        .build();

    // Test backward compatibility
    Assert.assertFalse(newSchema.isBackwardCompatibleWith(oldSchema));

    SchemaCompatibilityResult result = newSchema.checkBackwardCompatibilityWithDetails(oldSchema);
    Assert.assertFalse(result.isCompatible());
    Assert.assertEquals(result.getIssues().size(), 2);

    // Check detailed error message
    String detailedMessage = result.getDetailedErrorMessage();
    Assert.assertTrue(detailedMessage.contains("field type mismatch"));
    Assert.assertTrue(detailedMessage.contains("category") && detailedMessage.contains("DIMENSION")
        && detailedMessage.contains("METRIC"));
    Assert.assertTrue(detailedMessage.contains("count") && detailedMessage.contains("METRIC")
        && detailedMessage.contains("DIMENSION"));

    // Check summary error message
    String summaryMessage = result.getSummaryErrorMessage();
    Assert.assertTrue(summaryMessage.contains("field type mismatches [category, count]"));
  }

  @Test
  public void testSingleMultiValueMismatchIncompatibility() {
    // Create old schema
    Schema oldSchema = new Schema.SchemaBuilder()
        .setSchemaName("test_table")
        .addSingleValueDimension("user_id", FieldSpec.DataType.STRING)
        .addMultiValueDimension("tags", FieldSpec.DataType.STRING)
        .addSingleValueDimension("status", FieldSpec.DataType.STRING)
        .build();

    // Create new schema with single/multi-value changes
    Schema newSchema = new Schema.SchemaBuilder()
        .setSchemaName("test_table")
        .addSingleValueDimension("user_id", FieldSpec.DataType.STRING)
        .addSingleValueDimension("tags", FieldSpec.DataType.STRING) // Changed multi-value to single-value
        .addMultiValueDimension("status", FieldSpec.DataType.STRING) // Changed single-value to multi-value
        .build();

    // Test backward compatibility
    Assert.assertFalse(newSchema.isBackwardCompatibleWith(oldSchema));

    SchemaCompatibilityResult result = newSchema.checkBackwardCompatibilityWithDetails(oldSchema);
    Assert.assertFalse(result.isCompatible());
    Assert.assertEquals(result.getIssues().size(), 2);

    // Check detailed error message
    String detailedMessage = result.getDetailedErrorMessage();
    Assert.assertTrue(detailedMessage.contains("single/multi-value mismatch"));
    Assert.assertTrue(detailedMessage.contains("tags") && detailedMessage.contains("multi-value")
        && detailedMessage.contains("single-value"));
    Assert.assertTrue(detailedMessage.contains("status") && detailedMessage.contains("single-value")
        && detailedMessage.contains("multi-value"));
    Assert.assertTrue(detailedMessage.contains("💡 Suggestion"));

    // Check summary error message
    String summaryMessage = result.getSummaryErrorMessage();
    Assert.assertTrue(summaryMessage.contains("single/multi-value mismatches [status, tags]"));
  }

  @Test
  public void testMultipleTypesOfIncompatibilities() {
    // Create old schema
    Schema oldSchema = new Schema.SchemaBuilder()
        .setSchemaName("test_table")
        .addSingleValueDimension("user_id", FieldSpec.DataType.STRING)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .addSingleValueDimension("category", FieldSpec.DataType.STRING)
        .addMultiValueDimension("tags", FieldSpec.DataType.STRING)
        .addMetric("age", FieldSpec.DataType.INT)
        .addMetric("score", FieldSpec.DataType.DOUBLE)
        .build();

    // Create new schema with multiple types of incompatibilities
    Schema newSchema = new Schema.SchemaBuilder()
        .setSchemaName("test_table")
        .addSingleValueDimension("user_id", FieldSpec.DataType.STRING)
        // Missing: name (MISSING_COLUMN)
        .addMetric("category", FieldSpec.DataType.INT) // Changed DIMENSION to METRIC
        .addSingleValueDimension("tags", FieldSpec.DataType.STRING) // Changed multi-value to single-value
        .addMetric("age", FieldSpec.DataType.LONG) // Changed INT to LONG (DATA_TYPE_MISMATCH)
        .addMetric("score", FieldSpec.DataType.DOUBLE)
        .build();

    // Test backward compatibility
    Assert.assertFalse(newSchema.isBackwardCompatibleWith(oldSchema));

    SchemaCompatibilityResult result = newSchema.checkBackwardCompatibilityWithDetails(oldSchema);
    Assert.assertFalse(result.isCompatible());
    Assert.assertEquals(result.getIssues().size(), 4);

    // Check that all error types are present
    String detailedMessage = result.getDetailedErrorMessage();
    Assert.assertTrue(detailedMessage.contains("Found 4 issue(s)"));

    // Check summary groups issues correctly
    String summaryMessage = result.getSummaryErrorMessage();
    Assert.assertTrue(summaryMessage.contains("missing columns [name]"));
    Assert.assertTrue(summaryMessage.contains("data type mismatches [age]"));
    Assert.assertTrue(summaryMessage.contains("field type mismatches [category]"));
    Assert.assertTrue(summaryMessage.contains("single/multi-value mismatches [tags]"));
  }

  @Test
  public void testDataTypeSuggestions() {
    // Create old schema with INT
    Schema oldSchema = new Schema.SchemaBuilder()
        .setSchemaName("test_table")
        .addMetric("value", FieldSpec.DataType.INT)
        .build();

    // Create new schema with LONG (safe upgrade)
    Schema newSchema = new Schema.SchemaBuilder()
        .setSchemaName("test_table")
        .addMetric("value", FieldSpec.DataType.LONG)
        .build();

    SchemaCompatibilityResult result = newSchema.checkBackwardCompatibilityWithDetails(oldSchema);
    Assert.assertFalse(result.isCompatible());
    
    String detailedMessage = result.getDetailedErrorMessage();
    Assert.assertTrue(detailedMessage.contains("INT to LONG is generally safe for most use cases"));
  }

  @Test
  public void testFieldTypeSuggestions() {
    // Create old schema with DIMENSION
    Schema oldSchema = new Schema.SchemaBuilder()
        .setSchemaName("test_table")
        .addSingleValueDimension("value", FieldSpec.DataType.INT)
        .build();

    // Create new schema with METRIC 
    Schema newSchema = new Schema.SchemaBuilder()
        .setSchemaName("test_table")
        .addMetric("value", FieldSpec.DataType.INT)
        .build();

    SchemaCompatibilityResult result = newSchema.checkBackwardCompatibilityWithDetails(oldSchema);
    Assert.assertFalse(result.isCompatible());
    
    String detailedMessage = result.getDetailedErrorMessage();
    Assert.assertTrue(detailedMessage.contains("Converting DIMENSION to METRIC may affect query performance"));
  }
}
