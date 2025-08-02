package org.apache.pinot.client;

import org.testng.annotations.Test;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.*;

public class PinotResultMetadataTest {

  private PinotResultMetadata createMetadata() {
    // 2 columns: "colA" at index 1, "colB" at index 2
    Map<String, Integer> columnsNameToIndex = new HashMap<>();
    columnsNameToIndex.put("colA", 1);
    columnsNameToIndex.put("colB", 2);
    Map<Integer, String> columnDataTypes = new HashMap<>();
    columnDataTypes.put(1, "STRING");
    columnDataTypes.put(2, "INT");
    return new PinotResultMetadata(2, columnsNameToIndex, columnDataTypes);
}

  @Test
  public void testGetColumnCount() throws SQLException {
    PinotResultMetadata meta = createMetadata();
    assertEquals(meta.getColumnCount(), 2);
  }

  @Test
  public void testGetColumnName() throws SQLException {
    PinotResultMetadata meta = createMetadata();
    assertEquals(meta.getColumnName(1), "colA");
    assertEquals(meta.getColumnName(2), "colB");
    // Nonexistent column index > totalColumns should throw SQLException
  }

  @Test(expectedExceptions = SQLException.class)
  public void testGetColumnNameInvalidColumnThrows() throws SQLException {
    PinotResultMetadata meta = createMetadata();
    meta.getColumnName(3);
  }

  @Test
  public void testGetColumnTypeName() throws SQLException {
    PinotResultMetadata meta = createMetadata();
    assertEquals(meta.getColumnTypeName(1), "STRING");
    assertEquals(meta.getColumnTypeName(2), "INT");
    // Nonexistent column index > totalColumns should throw SQLException
  }

  @Test(expectedExceptions = SQLException.class)
  public void testGetColumnTypeNameInvalidColumnThrows2() throws SQLException {
    PinotResultMetadata meta = createMetadata();
    meta.getColumnTypeName(3);
  }

  @Test
  public void testGetColumnClassNameAndType() throws SQLException {
    PinotResultMetadata meta = createMetadata();
    // These depend on DriverUtils, but should not throw
    assertNotNull(meta.getColumnClassName(1));
    assertNotNull(meta.getColumnClassName(2));
    assertTrue(meta.getColumnType(1) >= 0); // Should return a valid SQL type int
    assertTrue(meta.getColumnType(2) >= 0);
  }

  @Test(expectedExceptions = SQLException.class)
  public void testInvalidColumnThrows() throws SQLException {
    PinotResultMetadata meta = createMetadata();
    // Should throw because column 5 > totalColumns (2)
    meta.getColumnName(5);
  }

  @Test(expectedExceptions = SQLException.class)
  public void testGetColumnTypeNameInvalidColumnThrows() throws SQLException {
    PinotResultMetadata meta = createMetadata();
    // Should throw because column 10 > totalColumns (2)
    meta.getColumnTypeName(10);
  }

}