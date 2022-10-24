package org.apache.pinot.integration.tests.postgres;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;


public class PinotTestDriver {
  private Connection _connection;
  private String _setUp =
      "/Users/yao/workspace/pinot/pinot-integration-tests/src/test/java/org/apache/pinot/integration/tests/postgres"
          + "/sql/test_setup.sql";
  private String _testDir =
      "/Users/yao/workspace/pinot/pinot-integration-tests/src/test/java/org/apache/pinot/integration/tests/postgres"
          + "/sql";

  private String _testFile =
      "/Users/yao/workspace/pinot/pinot-integration-tests/src/test/java/org/apache/pinot/integration/tests/postgres"
          + "/sql/join.sql";

  @Before
  public void setUp()
      throws Exception {
    _connection =
        DriverManager.getConnection("jdbc:postgresql://localhost:5432/myPostgresDb", "postgresUser", "postgresPW");
  }

  @After
  public void shutDown()
      throws Exception {
    _connection.close();
  }

  @Test
  public void testEmbeddedPg()
      throws Exception {
    File f = new File(_setUp);
      try (BufferedReader br = new BufferedReader(new FileReader(f))) {
        Scanner read = new Scanner(br);
        read.useDelimiter(";");
        while (read.hasNext()) {
          String text = read.next();
          System.out.println(text);
          if(text.contains("CREATE")){
            Statement stmt = _connection.createStatement();
            stmt.executeUpdate(text + ";");
            stmt.close();
          }
          System.out.println("=======================================");
//          PreparedStatement statement = _connection.prepareStatement(read.next() + ";");
//          System.out.println(statement);
//          statement.executeQuery();
        }
      }


    String SQL_SELECT = "Select * From CHAR_TBL";
    try (PreparedStatement preparedStatement = _connection.prepareStatement(SQL_SELECT)) {
      ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        System.out.println("================================result==================================================");
        System.out.println(resultSet.getLong(1));
      }
    } catch (SQLException e) {
      System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
