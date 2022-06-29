package org.apache.pinot.controller.recommender.rules.impl;

import org.apache.pinot.controller.recommender.exceptions.InvalidInputException;
import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.controller.recommender.rules.AbstractRule;
import org.apache.pinot.controller.recommender.rules.io.configs.IndexConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JSON columns must be NoDictionary columns with JsonIndex. */
public class JsonIndexRule extends AbstractRule {
  private static final Logger LOGGER = LoggerFactory.getLogger(RangeIndexRule.class);

  public JsonIndexRule(InputManager input, ConfigManager output) {
    super(input, output);
  }

  @Override
  public void run()
      throws InvalidInputException {
    int numColumns = _input.getNumCols();
    IndexConfig indexConfig = _output.getIndexConfig();
    for (int i = 0; i < numColumns; i++) {
      String columnName = _input.intToColName(i);
      FieldSpec.DataType columnType = _input.getFieldType(columnName);
      if (columnType == FieldSpec.DataType.JSON) {
        // JSON columns must be NoDictionary columns and have a JsonIndex.
        LOGGER.info("Recommending NoDictionary and JsonIndex on JSON column {}.", columnName);
        indexConfig.getJsonIndexColumns().add(columnName);
        indexConfig.getNoDictionaryColumns().add(columnName);
      }
    }
  }
}
