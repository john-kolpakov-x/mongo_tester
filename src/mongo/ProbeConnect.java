package mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class ProbeConnect {
  public static void main(String[] args) {
    try (MongoClient mongoClient = ConnectManager.get()) {

      MongoDatabase loadTestDb = mongoClient.getDatabase("LoadTest");

      MongoCollection<Document> tst1 = loadTestDb.getCollection("tst1");

      System.out.println(tst1);
    }
  }
}
