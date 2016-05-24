package mysql;

import java.sql.Connection;
import java.sql.DriverManager;

public class MysqlConnection {
  public static Connection get() throws Exception {
    Class.forName("com.mysql.jdbc.Driver");
    return DriverManager.getConnection("jdbc:mysql://localhost/asd", "asd", "asd");
  }
}
