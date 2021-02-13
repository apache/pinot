package org.apache.pinot.util.validator;

import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.apache.pinot.core.util.validator.BasePinotConfigValidator;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BasePinotConfigValidatorTest extends BasePinotConfigValidator {

  @Test
  public void testLoadingSchema()
      throws IOException, ProcessingException {
    // _schema object is null currently
    Assert.assertNull(_schema);

    loadSchema("/schemas/tableConfig.json");
    // _schema is store a valid schema after loading
    Assert.assertNotNull(_schema);

    // fail and throw IOException if schema not found
    try {
      loadSchema("/no/schema/here");
      Assert.fail("schema not found");
    } catch (IOException e) {
      // expected
    }
  }

  @Test
  public void testValidateSchema()
      throws IOException, ProcessingException {

    // given a valid schema, validate should returns true with not error messages
    String airlinesTableConfig = readSampleJson("/schemas/good_airlines_table_config.json");
    loadSchema("/schemas/tableConfig.json");
    boolean result = validate(airlinesTableConfig);
    Assert.assertEquals(getValidationMessages().size(), 0);
    Assert.assertTrue(result);

    // given a invalid schema [invalid key is table_type], validate should returns false
    airlinesTableConfig = readSampleJson("/schemas/bad_airlines_table_config.json");
    loadSchema("/schemas/tableConfig.json");
    result = validate(airlinesTableConfig);
    Assert.assertEquals(getValidationMessages().size(), 1);
    Assert.assertFalse(result);
  }

  private String readSampleJson(String path)
      throws IOException {
    StringBuilder sb = new StringBuilder();
    BufferedReader
        br = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(path), StandardCharsets.UTF_8));
    for (int c = br.read(); c != -1; c = br.read()) sb.append((char) c);

    return sb.toString();
  }
}
