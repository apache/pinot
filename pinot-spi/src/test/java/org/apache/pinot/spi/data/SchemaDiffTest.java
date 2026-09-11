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

import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


public class SchemaDiffTest {

  @Test
  public void testDeleteOneColumn() {
    Schema oldSchema = baseSchemaBuilder().addSingleValueDimension("toDelete", FieldSpec.DataType.INT).build();
    Schema newSchema = baseSchemaBuilder().build();

    SchemaDiff diff = SchemaDiff.compute(oldSchema, newSchema);
    assertThat(diff.getDeletedColumnNames()).containsExactly("toDelete");
    assertThat(diff.getAddedColumnNames()).isEmpty();
    assertThat(diff.getRetainedIncompatibleColumns()).isEmpty();
    assertThat(diff.isPrimaryKeyColumnsChanged()).isFalse();
    assertThat(diff.isExistingPrimaryKeyColumnsChanged()).isFalse();
    assertThat(diff.isCompatibleWhenColumnDeletionAllowed()).isTrue();
    assertThat(diff.isStructurallyUnchanged()).isFalse();
    assertThat(newSchema.isBackwardCompatibleWith(oldSchema)).isFalse();
  }

  @Test
  public void testDeleteManyColumns() {
    Schema oldSchema = baseSchemaBuilder().addSingleValueDimension("a", FieldSpec.DataType.INT)
        .addSingleValueDimension("b", FieldSpec.DataType.STRING).addMetric("c", FieldSpec.DataType.LONG).build();
    Schema newSchema = baseSchemaBuilder().build();

    SchemaDiff diff = SchemaDiff.compute(oldSchema, newSchema);
    assertThat(diff.getDeletedColumnNames()).containsExactly("a", "b", "c");
    assertThat(diff.isCompatibleWhenColumnDeletionAllowed()).isTrue();
  }

  @Test
  public void testAddPlusDelete() {
    Schema oldSchema = baseSchemaBuilder().addSingleValueDimension("oldCol", FieldSpec.DataType.INT).build();
    Schema newSchema = baseSchemaBuilder().addSingleValueDimension("newCol", FieldSpec.DataType.INT).build();

    SchemaDiff diff = SchemaDiff.compute(oldSchema, newSchema);
    assertThat(diff.getDeletedColumnNames()).containsExactly("oldCol");
    assertThat(diff.getAddedColumnNames()).containsExactly("newCol");
    assertThat(diff.getRetainedIncompatibleColumns()).isEmpty();
    assertThat(diff.isCompatibleWhenColumnDeletionAllowed()).isTrue();
  }

  @Test
  public void testRenameIsAddPlusDelete() {
    Schema oldSchema = baseSchemaBuilder().addSingleValueDimension("city", FieldSpec.DataType.STRING).build();
    Schema newSchema = baseSchemaBuilder().addSingleValueDimension("cityName", FieldSpec.DataType.STRING).build();

    SchemaDiff diff = SchemaDiff.compute(oldSchema, newSchema);
    assertThat(diff.getDeletedColumnNames()).containsExactly("city");
    assertThat(diff.getAddedColumnNames()).containsExactly("cityName");
    assertThat(diff.getRetainedIncompatibleColumns()).isEmpty();
  }

  @Test
  public void testRetainedTypeChange() {
    Schema oldSchema = baseSchemaBuilder().addSingleValueDimension("sv", FieldSpec.DataType.INT).build();
    Schema newSchema = baseSchemaBuilder().addSingleValueDimension("sv", FieldSpec.DataType.LONG).build();

    SchemaDiff diff = SchemaDiff.compute(oldSchema, newSchema);
    assertThat(diff.getDeletedColumnNames()).isEmpty();
    assertThat(diff.getAddedColumnNames()).isEmpty();
    assertThat(diff.getRetainedIncompatibleColumns()).hasSize(1);
    assertThat(diff.getRetainedIncompatibleColumns().get(0).getOldColumnName()).isEqualTo("sv");
    assertThat(diff.getRetainedIncompatibleColumns().get(0).getOldFieldSpec().getDataType())
        .isEqualTo(FieldSpec.DataType.INT);
    assertThat(diff.getRetainedIncompatibleColumns().get(0).getNewFieldSpec().getDataType())
        .isEqualTo(FieldSpec.DataType.LONG);
    assertThat(diff.isCompatibleWhenColumnDeletionAllowed()).isFalse();
    assertThat(newSchema.isBackwardCompatibleWith(oldSchema)).isFalse();
  }

  @Test
  public void testRetainedSingleValueToMultiValue() {
    Schema oldSchema = baseSchemaBuilder().addSingleValueDimension("tags", FieldSpec.DataType.STRING).build();
    Schema newSchema = baseSchemaBuilder().addMultiValueDimension("tags", FieldSpec.DataType.STRING).build();

    SchemaDiff diff = SchemaDiff.compute(oldSchema, newSchema);
    assertThat(diff.getRetainedIncompatibleColumns()).hasSize(1);
    assertThat(diff.isCompatibleWhenColumnDeletionAllowed()).isFalse();
  }

  @Test
  public void testRetainedFieldKindChange() {
    Schema oldSchema = baseSchemaBuilder().addSingleValueDimension("count", FieldSpec.DataType.INT).build();
    Schema newSchema = baseSchemaBuilder().addMetric("count", FieldSpec.DataType.INT).build();

    SchemaDiff diff = SchemaDiff.compute(oldSchema, newSchema);
    assertThat(diff.getRetainedIncompatibleColumns()).hasSize(1);
    assertThat(diff.getRetainedIncompatibleColumns().get(0).getOldFieldSpec().getFieldType())
        .isEqualTo(FieldSpec.FieldType.DIMENSION);
    assertThat(diff.getRetainedIncompatibleColumns().get(0).getNewFieldSpec().getFieldType())
        .isEqualTo(FieldSpec.FieldType.METRIC);
    assertThat(diff.isCompatibleWhenColumnDeletionAllowed()).isFalse();
  }

  @Test
  public void testDefaultValueChangeIsCompatible() {
    Schema oldSchema =
        baseSchemaBuilder().addSingleValueDimension("sv", FieldSpec.DataType.INT, 10).build();
    Schema newSchema =
        baseSchemaBuilder().addSingleValueDimension("sv", FieldSpec.DataType.INT, 100).build();

    SchemaDiff diff = SchemaDiff.compute(oldSchema, newSchema);
    assertThat(diff.getRetainedIncompatibleColumns()).isEmpty();
    assertThat(diff.isCompatibleWhenColumnDeletionAllowed()).isTrue();
    assertThat(diff.isStructurallyUnchanged()).isTrue();
    assertThat(newSchema.isBackwardCompatibleWith(oldSchema)).isTrue();
  }

  @Test
  public void testAddPrimaryKeysWhenNoneExisted() {
    Schema oldSchema = baseSchemaBuilder().build();
    Schema newSchema = baseSchemaBuilder().setPrimaryKeyColumns(List.of("id")).build();

    SchemaDiff diff = SchemaDiff.compute(oldSchema, newSchema);
    assertThat(diff.isPrimaryKeyColumnsChanged()).isTrue();
    assertThat(diff.isExistingPrimaryKeyColumnsChanged()).isFalse();
    assertThat(diff.isCompatibleWhenColumnDeletionAllowed()).isTrue();
    assertThat(newSchema.isBackwardCompatibleWith(oldSchema)).isTrue();
  }

  @Test
  public void testChangeExistingPrimaryKeys() {
    Schema oldSchema = baseSchemaBuilder().setPrimaryKeyColumns(List.of("id")).build();
    Schema newSchema = baseSchemaBuilder().setPrimaryKeyColumns(List.of("id", "country")).build();

    SchemaDiff diff = SchemaDiff.compute(oldSchema, newSchema);
    assertThat(diff.getOldPrimaryKeyColumns()).containsExactly("id");
    assertThat(diff.getNewPrimaryKeyColumns()).containsExactly("id", "country");
    assertThat(diff.isPrimaryKeyColumnsChanged()).isTrue();
    assertThat(diff.isExistingPrimaryKeyColumnsChanged()).isTrue();
    assertThat(diff.isCompatibleWhenColumnDeletionAllowed()).isFalse();
    assertThat(newSchema.isBackwardCompatibleWith(oldSchema)).isFalse();
  }

  @Test
  public void testPrimaryKeyCaseChangeIsReportedEvenWhenIgnoreCase() {
    Schema oldSchema = baseSchemaBuilder().setPrimaryKeyColumns(List.of("id")).build();
    Schema newSchema = baseSchemaBuilder().setPrimaryKeyColumns(List.of("ID")).build();

    SchemaDiff ignoreCase = SchemaDiff.compute(oldSchema, newSchema, true);
    assertThat(ignoreCase.isExistingPrimaryKeyColumnsChanged()).isTrue();
    assertThat(ignoreCase.isCompatibleWhenColumnDeletionAllowed()).isFalse();
    assertThat(ignoreCase.getDeletedColumnNames()).isEmpty();
    assertThat(ignoreCase.getAddedColumnNames()).isEmpty();
    assertThat(newSchema.isBackwardCompatibleWith(oldSchema)).isFalse();

    SchemaDiff exact = SchemaDiff.compute(oldSchema, newSchema, false);
    assertThat(exact.isExistingPrimaryKeyColumnsChanged()).isTrue();
    assertThat(exact.isCompatibleWhenColumnDeletionAllowed()).isFalse();
  }

  @Test
  public void testCaseSensitiveTreatsRenameAsAddAndDelete() {
    Schema oldSchema = baseSchemaBuilder().addSingleValueDimension("City", FieldSpec.DataType.STRING).build();
    Schema newSchema = baseSchemaBuilder().addSingleValueDimension("city", FieldSpec.DataType.STRING).build();

    SchemaDiff diff = SchemaDiff.compute(oldSchema, newSchema, false);
    assertThat(diff.getDeletedColumnNames()).containsExactly("City");
    assertThat(diff.getAddedColumnNames()).containsExactly("city");
    assertThat(diff.getRetainedIncompatibleColumns()).isEmpty();
  }

  @Test
  public void testIgnoreCaseMatchesSameColumn() {
    Schema oldSchema = baseSchemaBuilder().addSingleValueDimension("City", FieldSpec.DataType.STRING).build();
    Schema newSchema = baseSchemaBuilder().addSingleValueDimension("city", FieldSpec.DataType.STRING).build();

    SchemaDiff diff = SchemaDiff.compute(oldSchema, newSchema, true);
    assertThat(diff.getDeletedColumnNames()).isEmpty();
    assertThat(diff.getAddedColumnNames()).isEmpty();
    // FieldSpec compatibility compares the stored name, so a case-only rename is retained-incompatible.
    assertThat(diff.getRetainedIncompatibleColumns()).hasSize(1);
    assertThat(diff.getRetainedIncompatibleColumns().get(0).getOldColumnName()).isEqualTo("City");
    assertThat(diff.getRetainedIncompatibleColumns().get(0).getNewColumnName()).isEqualTo("city");
    assertThat(diff.isIgnoreCase()).isTrue();
  }

  @Test
  public void testIgnoreCaseTypeChange() {
    Schema oldSchema = baseSchemaBuilder().addSingleValueDimension("City", FieldSpec.DataType.STRING).build();
    Schema newSchema = baseSchemaBuilder().addSingleValueDimension("city", FieldSpec.DataType.INT).build();

    SchemaDiff diff = SchemaDiff.compute(oldSchema, newSchema, true);
    assertThat(diff.getDeletedColumnNames()).isEmpty();
    assertThat(diff.getRetainedIncompatibleColumns()).hasSize(1);
    assertThat(diff.isCompatibleWhenColumnDeletionAllowed()).isFalse();
  }

  @Test
  public void testIgnoreCaseCollisionThrows() {
    Schema colliding = new Schema.SchemaBuilder().setSchemaName("collide")
        .addSingleValueDimension("City", FieldSpec.DataType.STRING)
        .addSingleValueDimension("city", FieldSpec.DataType.INT).build();
    Schema other = baseSchemaBuilder().build();

    assertThatThrownBy(() -> SchemaDiff.compute(colliding, other, true)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("case-colliding");
  }

  @Test
  public void testNoOpIdenticalSchemas() {
    Schema oldSchema = baseSchemaBuilder().build();
    Schema newSchema = baseSchemaBuilder().build();

    SchemaDiff diff = SchemaDiff.compute(oldSchema, newSchema);
    assertThat(diff.isStructurallyUnchanged()).isTrue();
    assertThat(diff.getDeletedColumnNames()).isEmpty();
    assertThat(diff.isCompatibleWhenColumnDeletionAllowed()).isTrue();
    assertThat(newSchema.isBackwardCompatibleWith(oldSchema)).isTrue();
  }

  @Test
  public void testAddOnlyIsCompatibleWhenDeletionAllowed() {
    Schema oldSchema = baseSchemaBuilder().build();
    Schema newSchema = baseSchemaBuilder().addSingleValueDimension("extra", FieldSpec.DataType.INT).build();

    SchemaDiff diff = SchemaDiff.compute(oldSchema, newSchema);
    assertThat(diff.getDeletedColumnNames()).isEmpty();
    assertThat(diff.getAddedColumnNames()).containsExactly("extra");
    assertThat(diff.isCompatibleWhenColumnDeletionAllowed()).isTrue();
    assertThat(newSchema.isBackwardCompatibleWith(oldSchema)).isTrue();
  }

  @Test
  public void testDeletePlusTypeChangeIsNotCompatibleWhenDeletionAllowed() {
    Schema oldSchema = baseSchemaBuilder().addSingleValueDimension("gone", FieldSpec.DataType.INT)
        .addSingleValueDimension("sv", FieldSpec.DataType.INT).build();
    Schema newSchema = baseSchemaBuilder().addSingleValueDimension("sv", FieldSpec.DataType.LONG).build();

    SchemaDiff diff = SchemaDiff.compute(oldSchema, newSchema);
    assertThat(diff.getDeletedColumnNames()).containsExactly("gone");
    assertThat(diff.getRetainedIncompatibleColumns()).hasSize(1);
    assertThat(diff.isCompatibleWhenColumnDeletionAllowed()).isFalse();
  }

  @Test
  public void testOpenStructDelete() {
    Schema oldSchema = baseSchemaBuilder()
        .addOpenStruct("attrs", Map.of("count", new DimensionFieldSpec("count", FieldSpec.DataType.INT, true)))
        .build();
    Schema newSchema = baseSchemaBuilder().build();

    SchemaDiff diff = SchemaDiff.compute(oldSchema, newSchema);
    assertThat(diff.getDeletedColumnNames()).containsExactly("attrs");
    assertThat(diff.isCompatibleWhenColumnDeletionAllowed()).isTrue();
  }

  @Test
  public void testNullSchemaRejected() {
    Schema schema = baseSchemaBuilder().build();
    assertThatThrownBy(() -> SchemaDiff.compute(null, schema)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> SchemaDiff.compute(schema, null)).isInstanceOf(NullPointerException.class);
  }

  private static Schema.SchemaBuilder baseSchemaBuilder() {
    return new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addSingleValueDimension("id", FieldSpec.DataType.INT)
        .addSingleValueDimension("country", FieldSpec.DataType.STRING)
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS");
  }
}
