package org.apache.pinot.integration.tests.postgres;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import org.junit.rules.TemporaryFolder;


public class PinotTestDriver {
  @Test
  public void testEmbeddedPg()
      throws Exception {
    String SQL_SELECT = "Select 1";
    // auto close connection and preparedStatement
    try (Connection conn = DriverManager.getConnection("http://localhost:55000", null, null);
        PreparedStatement preparedStatement = conn.prepareStatement(SQL_SELECT)) {

      ResultSet resultSet = preparedStatement.executeQuery();

      while (resultSet.next()) {
        System.out.println(resultSet.getString(1));
      }
    } catch (SQLException e) {
      System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
