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
package com.linkedin.pinot.util;

import com.linkedin.pinot.common.utils.DataTableBuilderTest;
import com.linkedin.pinot.common.utils.DataTableJavaSerDe;
import com.linkedin.pinot.common.utils.DataTableSerDe;
import com.linkedin.pinot.common.utils.DataTableSerDeRegistry;
import com.linkedin.pinot.core.util.DataTableCustomSerDe;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests for DataTableBuilder class.
 * Registers {@link DataTableCustomSerDe} ser/de and runs the tests
 * in {@link DataTableBuilderTest}.
 */
public class DataTableBuilderWithCustomSerDeTest extends DataTableBuilderTest {

  private DataTableSerDe javaSerDe;
  private DataTableSerDe customSerDe;
  /**
   * Setup before all tests.
   * <p> - Sets up DataTableCustomSerDe for ser/de of data table.</p>
   */
  @BeforeClass
  public void setup() {
    javaSerDe = new DataTableJavaSerDe();
    customSerDe = new DataTableCustomSerDe();

    DataTableSerDeRegistry.getInstance().register(customSerDe);
    super.setup();
  }

  @Test
  public void testSimple()
      throws Exception {
    super.testSimple();

    // Backward compatibility test
    super.setDataTableSerDe(javaSerDe);
    super.testSimple();
  }

  @Test
  public void testStringArray() throws Exception {
    super.testStringArray();

    // Backward compatibility test
    super.setDataTableSerDe(javaSerDe);
    super.testStringArray();
  }

  @Test
  public void testIntArray() throws Exception {
    super.testIntArray();

    // Backward compatibility test
    super.setDataTableSerDe(javaSerDe);
    super.testIntArray();
  }

  @Test
  public void testComplexDataTypes() throws Exception {
    super.testComplexDataTypes();

    // Backward compatibility test
    super.setDataTableSerDe(javaSerDe);
    super.testComplexDataTypes();
  }

  @Test
  public void testSerDeSpeed() throws Exception {
    super.testSerDeserSpeed();

    // Backward compatibility test
    super.setDataTableSerDe(javaSerDe);
    super.testSerDeserSpeed();
  }
}
