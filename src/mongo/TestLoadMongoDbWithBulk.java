package mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import kz.greetgo.util.RND;
import org.bson.Document;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class TestLoadMongoDbWithBulk {

  public static final int THREADS_COUNT = 10;
  //public static final int BULK_SIZE = 10;
  public static final int BULK_SIZE = 10;

  public static final UpdateOptions UPSERT = new UpdateOptions().upsert(true);

  public static void main(String[] args) throws Exception {
    new TestLoadMongoDbWithBulk().execute();
    System.out.println("All Threads Complete");
  }

  class InsertThread extends Thread {

    int size = 0;
    long totalPeriod;
    boolean working = true, showCurrent = false, printIds = false;

    @Override
    public void run() {
      try (MongoClient mongoClient = ConnectManager.get()) {

        MongoDatabase loadTestDb = mongoClient.getDatabase("LoadTest");

        MongoCollection<Document> tst1 = loadTestDb.getCollection("tst1");

        int currentSize = 0;
        long started = System.currentTimeMillis();
        long startedPortion = started;


        while (working) {
          List<WriteModel<Document>> requests = new ArrayList<>();

          for (int b = 0; b < BULK_SIZE && working; b++) {

            String id = getTestId();

            BasicDBObject filter = new BasicDBObject();
            filter.put("id", id);

            BasicDBObject data = new BasicDBObject();
            for (int i = 1; i <= 300; i++) {
              data.put("name" + i, RND.str(70));
            }

            BasicDBObject setter = new BasicDBObject();
            setter.put("$set", data);

            //tst1.updateOne(filter, setter, UPSERT);

            UpdateOneModel<Document> up = new UpdateOneModel<>(filter, setter, UPSERT);
            requests.add(up);

            currentSize++;
            size++;

            if (showCurrent) {
              showCurrent = false;

              int s = currentSize;
              currentSize = 0;
              long now = System.currentTimeMillis();
              long period = now - startedPortion;
              startedPortion = now;

              System.out.println(Thread.currentThread().getName() + ": Upserted " + s + " records for "
                  + ((double) period / 1000.0) + " s; tps = " + tos(((double) s / (double) period * 1000.0)));
            }
            
            if (printIds) {
              printIds = true;
              idOuter.println(id);
            }
          }

          tst1.bulkWrite(requests);

        }

        long totalPeriod = System.currentTimeMillis() - started;

        System.out.println("FINISHED THREAD: " + Thread.currentThread().getName());
        System.out.println(Thread.currentThread().getName() + ": TOTAL Upserted " + size + " records, for "
            + ((double) totalPeriod / 1000.0) + " s; tps = " + tos(((double) size / (double) totalPeriod) * 1000.0));

        this.totalPeriod = totalPeriod;
      }
    }
  }

  private static String getTestId() {
    return RND.str(5);
  }

  private PrintStream idOuter;

  private void execute() throws Exception {
    final InsertThread tt[] = new InsertThread[THREADS_COUNT];
    for (int i = 0; i < THREADS_COUNT; i++) {
      tt[i] = new InsertThread();
    }

    final File signalFile = new File("build/__upload_going_on___");
    signalFile.getParentFile().mkdirs();
    signalFile.createNewFile();

    FileOutputStream fOut = new FileOutputStream("build/__ids__.txt", true);
    idOuter = new PrintStream(fOut, true, "UTF-8");

    final long totalStarted = System.currentTimeMillis();

    new Thread(new Runnable() {
      @Override
      public void run() {
        try {

          while (signalFile.exists()) {
            Thread.sleep(3000);

            long now = System.currentTimeMillis();

            int size = 0;
            for (InsertThread t : tt) {
              size += t.size;
              t.showCurrent = true;
              t.printIds = true;
            }

            System.out.println("------------------------------- uploaded records "
                + size + ", tps: " + tos(((double) (size) / (double) (now - totalStarted) * 1000.0)));

          }

          for (InsertThread t : tt) {
            t.working = false;
          }

        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }).start();

    for (InsertThread t : tt) {
      t.start();
    }
    for (InsertThread t : tt) {
      t.join();
    }

    long totalPeriod = System.currentTimeMillis() - totalStarted;

    int totalSize = 0;
    for (InsertThread t : tt) {
      totalSize += t.size;
    }

    System.out.println("ALL THREADS Upserted " + totalSize + " records, for " + totalPeriod + " ms;"
        + " tps = " + tos(((double) totalSize / (double) totalPeriod * 1000.0)));
    idOuter.close();
  }

  private String tos(double x) {
    return String.format("%4.2f", x);
  }
}
