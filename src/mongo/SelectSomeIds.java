package mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.PrintStream;

public class SelectSomeIds {
  public static void main(String[] args) throws Exception {
    try (MongoClient mongoClient = ConnectManager.get()) {

      MongoDatabase loadTestDb = mongoClient.getDatabase("LoadTest");

      MongoCollection<Document> tst1 = loadTestDb.getCollection("tst1");

      FindIterable<Document> documents = tst1.find();
      int c = 0;

      try (PrintStream pr = new PrintStream("build/__ids__.txt", "UTF-8")) {

        int skip = 0;
        int count = 0;
        for (Document doc : documents) {
          count++;
          if (count < skip) continue;
          pr.println(doc.get("id"));
          if (count > skip + 100000) break;
          c++;
          if (c > 800) {
            System.out.println("count = " + count);
            c = 0;
          }
        }

      }
    }
  }

}
