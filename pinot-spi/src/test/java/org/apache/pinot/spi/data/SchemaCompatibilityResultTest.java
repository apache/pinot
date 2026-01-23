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

import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for SchemaCompatibilityResult to ensure proper error message formatting and handling
 */
public class SchemaCompatibilityResultTest {

  @Test
  public void testCompatibleResult() {
    SchemaCompatibilityResult result = SchemaCompatibilityResult.compatible();
    
    Assert.assertTrue(result.isCompatible());
    Assert.assertTrue(result.getIssues().isEmpty());
    Assert.assertEquals(result.getDetailedErrorMessage(), "Schema is backward compatible");
    Assert.assertEquals(result.getSummaryErrorMessage(), "Schema is backward compatible");
  }

  @Test
  public void testSingleMissingColumnError() {
    List<SchemaCompatibilityResult.IncompatibilityIssue> issues = new ArrayList<>();
    issues.add(new SchemaCompatibilityResult.IncompatibilityIssue(
        SchemaCompatibilityResult.ErrorCode.MISSING_COLUMN,
        "user_id",
        "Column 'user_id' exists in old schema but is missing in new schema",
        "Add the missing column back to the schema or ensure data migration handles the missing column appropriately."
    ));
    
    SchemaCompatibilityResult result = SchemaCompatibilityResult.incompatible(issues);
    
    Assert.assertFalse(result.isCompatible());
    Assert.assertEquals(result.getIssues().size(), 1);
    
    String detailedMessage = result.getDetailedErrorMessage();
    Assert.assertTrue(detailedMessage.contains("Found 1 issue(s)"));
    Assert.assertTrue(detailedMessage.contains("❌ Column 'user_id' exists in old schema but is missing in new schema"));
    Assert.assertTrue(detailedMessage.contains("💡 Suggestion: Add the missing column back to the schema"));
    
    String summaryMessage = result.getSummaryErrorMessage();
    Assert.assertTrue(summaryMessage.contains("missing columns [user_id]"));
  }

  @Test
  public void testDataTypeMismatchError() {
    List<SchemaCompatibilityResult.IncompatibilityIssue> issues = new ArrayList<>();
    issues.add(new SchemaCompatibilityResult.IncompatibilityIssue(
        SchemaCompatibilityResult.ErrorCode.DATA_TYPE_MISMATCH,
        "age",
        "Column 'age' has data type mismatch: old=INT, new=STRING",
        "Reverting data type from STRING back to INT, or implement a data migration strategy to handle the type change."
    ));
    
    SchemaCompatibilityResult result = SchemaCompatibilityResult.incompatible(issues);
    
    Assert.assertFalse(result.isCompatible());
    String detailedMessage = result.getDetailedErrorMessage();
    Assert.assertTrue(detailedMessage.contains("❌ Column 'age' has data type mismatch: old=INT, new=STRING"));
    Assert.assertTrue(detailedMessage.contains("💡 Suggestion: Reverting data type from STRING back to INT"));
    
    String summaryMessage = result.getSummaryErrorMessage();
    Assert.assertTrue(summaryMessage.contains("data type mismatches [age]"));
  }

  @Test
  public void testFieldTypeMismatchError() {
    List<SchemaCompatibilityResult.IncompatibilityIssue> issues = new ArrayList<>();
    issues.add(new SchemaCompatibilityResult.IncompatibilityIssue(
        SchemaCompatibilityResult.ErrorCode.FIELD_TYPE_MISMATCH,
        "score",
        "Column 'score' has field type mismatch: old=METRIC, new=DIMENSION",
        "Converting METRIC to DIMENSION is generally safe but may affect aggregation queries that depend on this column."
    ));
    
    SchemaCompatibilityResult result = SchemaCompatibilityResult.incompatible(issues);
    
    Assert.assertFalse(result.isCompatible());
    String detailedMessage = result.getDetailedErrorMessage();
    Assert.assertTrue(detailedMessage.contains("❌ Column 'score' has field type mismatch: old=METRIC, new=DIMENSION"));
    Assert.assertTrue(detailedMessage.contains("💡 Suggestion: Converting METRIC to DIMENSION is generally safe"));
    
    String summaryMessage = result.getSummaryErrorMessage();
    Assert.assertTrue(summaryMessage.contains("field type mismatches [score]"));
  }

  @Test
  public void testSingleMultiValueMismatchError() {
    List<SchemaCompatibilityResult.IncompatibilityIssue> issues = new ArrayList<>();
    issues.add(new SchemaCompatibilityResult.IncompatibilityIssue(
        SchemaCompatibilityResult.ErrorCode.SINGLE_MULTI_VALUE_MISMATCH,
        "tags",
        "Column 'tags' has single/multi-value mismatch: old=multi-value, new=single-value",
        "Converting multi-value to single-value may cause data loss. Implement aggregation logic (e.g., take first value, concatenate) before making this change."
    ));
    
    SchemaCompatibilityResult result = SchemaCompatibilityResult.incompatible(issues);
    
    Assert.assertFalse(result.isCompatible());
    String detailedMessage = result.getDetailedErrorMessage();
    Assert.assertTrue(detailedMessage.contains("❌ Column 'tags' has single/multi-value mismatch: old=multi-value, new=single-value"));
    Assert.assertTrue(detailedMessage.contains("💡 Suggestion: Converting multi-value to single-value may cause data loss"));
    
    String summaryMessage = result.getSummaryErrorMessage();
    Assert.assertTrue(summaryMessage.contains("single/multi-value mismatches [tags]"));
  }

  @Test
  public void testMultipleIssuesErrorMessage() {
    List<SchemaCompatibilityResult.IncompatibilityIssue> issues = new ArrayList<>();
    issues.add(new SchemaCompatibilityResult.IncompatibilityIssue(
        SchemaCompatibilityResult.ErrorCode.MISSING_COLUMN,
        "user_id",
        "Column 'user_id' exists in old schema but is missing in new schema",
        "Add the missing column back to the schema or ensure data migration handles the missing column appropriately."
    ));
    issues.add(new SchemaCompatibilityResult.IncompatibilityIssue(
        SchemaCompatibilityResult.ErrorCode.DATA_TYPE_MISMATCH,
        "age",
        "Column 'age' has data type mismatch: old=INT, new=STRING",
        "Reverting data type from STRING back to INT, or implement a data migration strategy to handle the type change."
    ));
    issues.add(new SchemaCompatibilityResult.IncompatibilityIssue(
        SchemaCompatibilityResult.ErrorCode.MISSING_COLUMN,
        "created_at",
        "Column 'created_at' exists in old schema but is missing in new schema",
        "Add the missing column back to the schema or ensure data migration handles the missing column appropriately."
    ));
    
    SchemaCompatibilityResult result = SchemaCompatibilityResult.incompatible(issues);
    
    Assert.assertFalse(result.isCompatible());
    Assert.assertEquals(result.getIssues().size(), 3);
    
    String detailedMessage = result.getDetailedErrorMessage();
    Assert.assertTrue(detailedMessage.contains("Found 3 issue(s)"));
    Assert.assertTrue(detailedMessage.contains("1. ❌ Column 'user_id' exists in old schema"));
    Assert.assertTrue(detailedMessage.contains("2. ❌ Column 'age' has data type mismatch"));
    Assert.assertTrue(detailedMessage.contains("3. ❌ Column 'created_at' exists in old schema"));
    
    String summaryMessage = result.getSummaryErrorMessage();
    Assert.assertTrue(summaryMessage.contains("missing columns [user_id, created_at]"));
    Assert.assertTrue(summaryMessage.contains("data type mismatches [age]"));
  }

  @Test
  public void testErrorCodeMessageTemplates() {
    // Test all error code message templates
    Assert.assertEquals(
        SchemaCompatibilityResult.ErrorCode.MISSING_COLUMN.getMessageTemplate(),
        "Column '%s' exists in old schema but is missing in new schema"
    );
    Assert.assertEquals(
        SchemaCompatibilityResult.ErrorCode.DATA_TYPE_MISMATCH.getMessageTemplate(),
        "Column '%s' has data type mismatch: old=%s, new=%s"
    );
    Assert.assertEquals(
        SchemaCompatibilityResult.ErrorCode.FIELD_TYPE_MISMATCH.getMessageTemplate(),
        "Column '%s' has field type mismatch: old=%s, new=%s"
    );
    Assert.assertEquals(
        SchemaCompatibilityResult.ErrorCode.SINGLE_MULTI_VALUE_MISMATCH.getMessageTemplate(),
        "Column '%s' has single/multi-value mismatch: old=%s, new=%s"
    );
  }

  @Test
  public void testIncompatibilityIssueToString() {
    SchemaCompatibilityResult.IncompatibilityIssue issue = new SchemaCompatibilityResult.IncompatibilityIssue(
        SchemaCompatibilityResult.ErrorCode.MISSING_COLUMN,
        "user_id",
        "Column 'user_id' exists in old schema but is missing in new schema",
        "Add the missing column back to the schema or ensure data migration handles the missing column appropriately."
    );
    
    String issueString = issue.toString();
    Assert.assertTrue(issueString.contains("❌ Column 'user_id' exists in old schema"));
    Assert.assertTrue(issueString.contains("💡 Suggestion: Add the missing column back"));
  }

  @Test 
  public void testIncompatibilityIssueWithoutSuggestion() {
    SchemaCompatibilityResult.IncompatibilityIssue issue = new SchemaCompatibilityResult.IncompatibilityIssue(
        SchemaCompatibilityResult.ErrorCode.MISSING_COLUMN,
        "user_id",
        "Column 'user_id' exists in old schema but is missing in new schema",
        null
    );
    
    String issueString = issue.toString();
    Assert.assertTrue(issueString.contains("❌ Column 'user_id' exists in old schema"));
    Assert.assertFalse(issueString.contains("💡 Suggestion:"));
  }
}
