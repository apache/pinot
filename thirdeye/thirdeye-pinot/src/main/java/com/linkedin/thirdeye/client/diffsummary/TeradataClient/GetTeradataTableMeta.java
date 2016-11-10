package com.linkedin.thirdeye.client.diffsummary.TeradataClient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


public class GetTeradataTableMeta {

  private String sUser;
  private String sPassword;
  private String url;
  private Connection con;
  private Statement _statement;

  public GetTeradataTableMeta(String sUser, String sPassword, String url) {
    //TODO: Refactor to use Datasource which will provide pooled connection
    this.sUser = sUser;
    this.sPassword = sPassword;
    this.url = url;
    try {
      con = DriverManager.getConnection(url, sUser, sPassword);
    } catch (SQLException e) {
      throw new IllegalArgumentException(e);
    }

    try {
      _statement = con.createStatement();
    } catch (SQLException e) {
      throw new IllegalArgumentException(e);
    }

  }

  public List<String> getMeta(String tableName) {
    String sSelAll = " SELECT top 1 * FROM " + tableName;
    List<String> colNames = new ArrayList<>();
    try {
      ResultSet rset = _statement.executeQuery(sSelAll);
      ResultSetMetaData rsmd = rset.getMetaData();
      int colCount = rsmd.getColumnCount();
      for (int i = 1; i <= colCount; i++) {
        colNames.add(rsmd.getColumnName(i));
      }
    } catch (SQLException ex) {
      throw new IllegalStateException(ex);
    }
    return colNames;
  }

  public void closeConnection() {
    try {
      if (!con.isClosed()) {
        if (!_statement.isClosed()) {
          _statement.close();
        }
        con.close();
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }


  public static void main(String args[]) throws ClassNotFoundException {
    String sUser = "tableau";
    String sPassword = "esv_r8p1db1t";
    String url = "jdbc:teradata://dwprod1.corp.linkedin.com/TMODE=ANSI,CHARSET=UTF8";
    GetTeradataTableMeta meta = new GetTeradataTableMeta(sUser, sPassword, url);

    List<String> colNames = meta.getMeta("dwh.v_dim_degree");

    colNames.forEach(System.out::println);

    System.out.println("School dimension: ");
    List<String> c1 = meta.getMeta("dwh.v_dim_school");

    c1.forEach(System.out::println);

    meta.closeConnection();
  }
}
