package org.apache.pinot.thirdeye.misc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;

public class RandomDataInjector {

    static final String JDBC_DRIVER = "org.postgresql.Driver";
    static final String DB_URL = "jdbc:postgresql://127.0.0.1:5432/thirdeye";

    //  Database credentials
    static final String USER = "postgres";
    static final String PASS = "root";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        ArrayList<String> paymentMethods = new ArrayList<>();
        paymentMethods.add("Prepaid Card");
        paymentMethods.add("Debit Card");
        paymentMethods.add("wallet");
        paymentMethods.add("nach");
        paymentMethods.add("UPI Collect");
        paymentMethods.add("Credit Card");
        paymentMethods.add("paylater");
        paymentMethods.add("UPI Intent");
        paymentMethods.add("netbanking");
        paymentMethods.add("UPI Unknown");
        paymentMethods.add("emandate");
        paymentMethods.add("Unknown Card");
        paymentMethods.add("cardless_emi");
        try {
            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("Connecting to a selected database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Inserting records into the table.");
            stmt = conn.createStatement();

            for (int i = 0; i < 10000; i++) {
                String date = LocalDateTime.now().minusDays(15).toString();
                date = date.replace('T', ' ');
                date = date.substring(0, 19);
                float sr = (float) Math.random();
                String sql = "insert into success_rate (payment_method, minute, sr) VALUES ('" +
                        paymentMethods.get((int) (Math.random() * 12)) + "','" + date + "', " + sr + ");";
                stmt.executeUpdate(sql);
                Thread.sleep(10000);
                if (i % 100 == 0) {
                    System.out.println(i + " : " + sql);
                }
            }
            System.out.println("Inserted records into the table...");

        } catch (SQLException se) {
            se.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null)
                    conn.close();
            } catch (SQLException se) {
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
        System.out.println("Done");
    }
}
