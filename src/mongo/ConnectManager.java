package mongo;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

import java.util.Arrays;

public class ConnectManager {
  public static MongoClient get1() {
    return new MongoClient(Arrays.asList(
        new ServerAddress("192.168.11.23", 31011),
        new ServerAddress("192.168.11.23", 31012)
    ));
  }

  public static MongoClient get() {
    return new MongoClient("127.0.0.1", 27017);
  }
}
