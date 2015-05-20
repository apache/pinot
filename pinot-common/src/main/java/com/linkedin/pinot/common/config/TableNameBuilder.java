package com.linkedin.pinot.common.config;

import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.StringUtil;


public class TableNameBuilder {

  TableType type;

  public TableNameBuilder(TableType type) {
    this.type = type;
  }

  public String forTable(String tableName) {
    if (needsPostfix(tableName)) {
      return StringUtil.join("_", tableName, type.toString().toUpperCase());
    }
    return tableName;
  }

  public boolean needsPostfix(String tableName) {
    return !tableName.endsWith(type.toString().toUpperCase());
  }
}
