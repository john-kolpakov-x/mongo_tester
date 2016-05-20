package mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import kz.greetgo.util.RND;
import org.bson.Document;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class StartUpsertManyOperations {

  public static final int THREADS_COUNT = 10;
  public static final int BULK_SIZE = 10;

  public static void main(String[] args) throws Exception {
    new StartUpsertManyOperations().execute();
  }

  final List<String> idList = new ArrayList<>();

  private void loadIds() throws IOException {
    File file = new File("build/__ids__.txt");
    try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"))) {
      while (true) {
        String line = br.readLine();
        if (line == null) break;
        if (line.length() == 0) continue;
        idList.add(line);
      }
    }
  }

  private final File goingOnFile = new File("build/__updates_is_going_on__");

  private void execute() throws Exception {
    loadIds();

    goingOnFile.createNewFile();

    System.out.println("Go....");

    testUpdates();
  }

  final ThreadUpdates threads[] = new ThreadUpdates[THREADS_COUNT];

  private void testUpdates() throws Exception {
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new ThreadUpdates();
    }

    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          viewer();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }).start();

    long started = System.currentTimeMillis();

    for (ThreadUpdates t : threads) {
      t.start();
    }

    for (ThreadUpdates t : threads) {
      t.join();
    }

    long totalTime = System.currentTimeMillis() - started;
    int totalCount = 0;
    for (ThreadUpdates t : threads) {
      totalCount += t.updateCount.get();
    }

    double tps = (double) totalCount / (double) totalTime * 1000.0;

    System.out.println("TOTAL TIME = " + (totalTime / 1000) + " s, COUNT = " + totalCount + ", TPS = " + tps);

    System.out.println("FINISH");
  }

  private void viewer() throws Exception {
    while (true) {

      for (ThreadUpdates t : threads) {
        t.cadreUpdateCount.set(0);
      }
      long cadreStarted = System.currentTimeMillis();

      Thread.sleep(3000);

      if (!goingOnFile.exists()) {
        for (ThreadUpdates t : threads) {
          t.working.set(false);
          t.showStatus.set(true);
        }
        return;
      }

      int count = 0;
      for (ThreadUpdates t : threads) {
        t.showStatus.set(true);
        count += t.cadreUpdateCount.get();
      }

      long cadreTime = System.currentTimeMillis() - cadreStarted;
      String tps = tos((double) count / (double) cadreTime * 1000.0);

      System.out.println("---------------------------------- count = " + count
          + ", cadreTime = " + cadreTime + ", tps = " + tps);
    }
  }

  class ThreadUpdates extends Thread {

    final AtomicInteger updateCount = new AtomicInteger(0);
    final AtomicInteger cadreUpdateCount = new AtomicInteger(0);
    final AtomicInteger myCadreUpdateCount = new AtomicInteger(0);

    AtomicBoolean working = new AtomicBoolean(true);
    AtomicBoolean showStatus = new AtomicBoolean(false);

    long lastStatusShow = System.currentTimeMillis();

    @Override
    public void run() {

      try (MongoClient mongoClient = ConnectManager.get()) {

        MongoDatabase loadTestDb = mongoClient.getDatabase("LoadTest");

        MongoCollection<Document> tst1 = loadTestDb.getCollection("tst1");

        while (working.get()) {

          List<WriteModel<Document>> requests = new ArrayList<>();

          for (int i = 0; i < BULK_SIZE && working.get(); i++) {

            BasicDBObject filter = new BasicDBObject();
            filter.put("id", idList.get(RND.plusInt(idList.size())));

            BasicDBObject data = new BasicDBObject();
            for (int j = 1; j <= 15; j++) {
              data.put("name" + j, RND.str(10));
            }

            BasicDBObject setter = new BasicDBObject();
            setter.put("$set", data);

            requests.add(new UpdateOneModel<Document>(filter, setter));
//            tst1.updateMany(filter, setter);

            updateCount.incrementAndGet();
            cadreUpdateCount.incrementAndGet();
            myCadreUpdateCount.incrementAndGet();

            if (showStatus.get()) showStatus();
          }

          tst1.bulkWrite(requests);
        }

        if (showStatus.get()) showStatus();
      }

    }

    private void showStatus() {
      showStatus.set(false);

      long last = lastStatusShow;
      lastStatusShow = System.currentTimeMillis();

      long cadreTime = lastStatusShow - last;
      int cadreCount = myCadreUpdateCount.getAndSet(0);
      String tps = tos((double) cadreCount / (double) cadreTime * 1000.0);

      System.out.println(Thread.currentThread().getName() + ": cadreTime = " + cadreTime
          + ", cadreCount = " + cadreCount + ", tps = " + tps);
    }
  }

  private String tos(double x) {
    return String.format("%4.2f", x);
  }

}
