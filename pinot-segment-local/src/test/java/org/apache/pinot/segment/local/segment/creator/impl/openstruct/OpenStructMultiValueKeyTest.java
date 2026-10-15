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
package org.apache.pinot.segment.local.segment.creator.impl.openstruct;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.OpenStructNaming;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// An OPEN_STRUCT key whose values are lists used to have nowhere to go: type inference does not map a collection,
/// so the key fell back to STRING and the column held the list's `toString`. Nothing inside it could be filtered
/// or counted.
///
/// A key is now materialized as a multi-value column when its values are collections. Shape follows the same rule
/// the type does — the declaration wins, and failing that the first value decides and then sticks — so a column
/// is never reshaped underneath documents that already wrote to it.
public class OpenStructMultiValueKeyTest {

  private static final String COLUMN = "props";
  private static final int NUM_DOCS = 10;

  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tempDir = Files.createTempDirectory("OpenStructMultiValueKeyTest").toFile();
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(_tempDir);
  }

  /// Seals a segment feeding `valuePerDoc.apply(docId)` for `key` on every doc, and returns the child's metadata.
  private ColumnMetadataImpl seal(String key, @Nullable FieldSpec declared, DocValue valuePerDoc)
      throws Exception {
    Map<String, FieldSpec> children = new HashMap<>();
    if (declared != null) {
      children.put(key, declared);
    }
    ComplexFieldSpec parent = new ComplexFieldSpec(COLUMN, DataType.OPEN_STRUCT, true, children);
    OpenStructColumnSplitter splitter = new OpenStructColumnSplitter(_tempDir, COLUMN, "testTable_OFFLINE", parent,
        new OpenStructIndexConfig(false, null, -1, null, 0.5, null, null, null, null));
    for (int d = 0; d < NUM_DOCS; d++) {
      Object value = valuePerDoc.apply(d);
      splitter.add(value == null ? Map.of() : Map.of(key, value), d);
    }
    splitter.seal();

    String materializedCol = OpenStructNaming.materializedColumnName(COLUMN, key);
    PropertiesConfiguration props = splitter.getMaterializedColumnMetadata().get(materializedCol);
    assertNotNull(props, "key must be materialized: " + key);
    return ColumnMetadataImpl.fromPropertiesConfiguration(props, NUM_DOCS, materializedCol);
  }

  private interface DocValue {
    @Nullable
    Object apply(int docId);
  }

  @Test
  public void testListValuesBecomeAMultiValueColumnOfTheElementType()
      throws Exception {
    ColumnMetadataImpl metadata = seal("tags", null, d -> List.of("a" + d, "b" + d));

    assertFalse(metadata.getFieldSpec().isSingleValueField(), "a key holding lists must be multi-value");
    assertEquals(metadata.getFieldSpec().getDataType(), DataType.STRING);
    assertEquals(metadata.getMaxNumberOfMultiValues(), 2);
    assertEquals(metadata.getTotalNumberOfEntries(), NUM_DOCS * 2);
  }

  @Test
  public void testNumericElementsKeepTheirType()
      throws Exception {
    ColumnMetadataImpl metadata = seal("codes", null, d -> List.of(d, d + 1, d + 2));

    assertFalse(metadata.getFieldSpec().isSingleValueField());
    assertEquals(metadata.getFieldSpec().getDataType(), DataType.INT);
    assertEquals(metadata.getMaxNumberOfMultiValues(), 3);
  }

  @Test
  public void testDisagreeingElementsResolveToString()
      throws Exception {
    // Same answer a key whose values drift across rows gets: STRING keeps every element rather than dropping the
    // ones that do not fit.
    ColumnMetadataImpl metadata = seal("mixed", null, d -> List.of(1, "two"));

    assertFalse(metadata.getFieldSpec().isSingleValueField());
    assertEquals(metadata.getFieldSpec().getDataType(), DataType.STRING);
  }

  @Test
  public void testDeclaredSingleValueKeyStaysSingleValueEvenWithListValues()
      throws Exception {
    // The declaration is what the user asked for, so it decides the shape — the values do not override it.
    ColumnMetadataImpl metadata = seal("tags",
        new DimensionFieldSpec("tags", DataType.STRING, true), d -> List.of(1, 2));

    assertTrue(metadata.getFieldSpec().isSingleValueField());
    assertEquals(metadata.getFieldSpec().getDataType(), DataType.STRING);
  }

  @Test
  public void testDeclaredMultiValueKeyIsMultiValueEvenWhenValuesAreScalars()
      throws Exception {
    ColumnMetadataImpl metadata = seal("tags",
        new DimensionFieldSpec("tags", DataType.STRING, false), d -> "only");

    assertFalse(metadata.getFieldSpec().isSingleValueField());
    // A scalar on a multi-value key is one value, not zero.
    assertEquals(metadata.getMaxNumberOfMultiValues(), 1);
  }

  @Test
  public void testScalarOnAMultiValueKeyBecomesOneElement()
      throws Exception {
    // First value is a list, so the key is multi-value; the scalars that follow are single-element values.
    ColumnMetadataImpl metadata = seal("tags", null, d -> d == 0 ? List.of("a", "b", "c") : "solo");

    assertFalse(metadata.getFieldSpec().isSingleValueField());
    assertEquals(metadata.getMaxNumberOfMultiValues(), 3);
    assertEquals(metadata.getTotalNumberOfEntries(), 3 + (NUM_DOCS - 1));
  }

  @Test
  public void testShapeIsDecidedByTheFirstValueAndSticks()
      throws Exception {
    // Scalar first, so the key is single-value and a later list is handled as any other value the column cannot
    // represent — it does not reshape a column other documents already wrote to.
    ColumnMetadataImpl metadata = seal("tags", null, d -> d == 0 ? "solo" : List.of("a", "b"));

    assertTrue(metadata.getFieldSpec().isSingleValueField());
  }

  @Test
  public void testAbsentDocsGetOneDefaultElement()
      throws Exception {
    // A multi-value column has no empty state on disk, so an absent doc holds one default — the same thing the
    // standard segment creator writes for an absent multi-value field.
    ColumnMetadataImpl metadata = seal("tags", null, d -> d < 6 ? List.of("a", "b") : null);

    assertFalse(metadata.getFieldSpec().isSingleValueField());
    assertEquals(metadata.getTotalNumberOfEntries(), 6 * 2 + (NUM_DOCS - 6));
  }

  @Test
  public void testEmptyListsAreNotAValue()
      throws Exception {
    // No elements means no value and no type to infer, and a materialized multi-value column has no empty state to
    // store — so an empty list is simply not the key being present, exactly as a null value is not.
    Map<String, FieldSpec> children = Map.of();
    ComplexFieldSpec parent = new ComplexFieldSpec(COLUMN, DataType.OPEN_STRUCT, true, children);
    OpenStructColumnSplitter splitter = new OpenStructColumnSplitter(_tempDir, COLUMN, "testTable_OFFLINE", parent,
        new OpenStructIndexConfig(false, null, -1, null, 0.5, null, null, null, null));
    for (int d = 0; d < NUM_DOCS; d++) {
      splitter.add(Map.of("tags", List.of()), d);
    }
    splitter.seal();

    assertTrue(splitter.getResolvedDenseKeys().isEmpty(), "a key that is never present is not a key");
    assertFalse(splitter.getMaterializedColumnMetadata()
        .containsKey(OpenStructNaming.materializedColumnName(COLUMN, "tags")));
  }

  @Test
  public void testEmptyListDoesNotEndTheKey()
      throws Exception {
    // An empty list in one document must not stop the key existing, nor change the shape the others established.
    ColumnMetadataImpl metadata = seal("tags", null, d -> d == 5 ? List.of() : List.of("a", "b"));

    assertFalse(metadata.getFieldSpec().isSingleValueField());
    assertEquals(metadata.getFieldSpec().getDataType(), DataType.STRING);
    // Nine real docs of two elements, plus the one default the empty doc reads as.
    assertEquals(metadata.getTotalNumberOfEntries(), 9 * 2 + 1);
  }
}
