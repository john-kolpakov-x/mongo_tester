package rocksdb;

import kz.greetgo.util.RND;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.File;

public class LoadDataN {
  //final static File dataDir = new File("/home/pompei/tmp/load_test_rocks_db");
  final static  File dataDir = new File("/home/pompei/discs/data2/load_test_rocks_db");

  public static final int THREADS_COUNT = 1;
  //public static final int BULK_SIZE = 10;
  public static final int BULK_SIZE = 10;

  final static String PRE_KEYS[] = {
      "4678912731942369874568934281764195786345761934856717653736524-",
      "3289749057789456328941695764325693174693281675943259867111224-",
      "4325614875604687596428765348167594150102809850185015856472834-",
  };
  static final int USE_KEYS = 1;


//  public static final UpdateOptions UPSERT = new UpdateOptions().upsert(true);

  static RocksDB db;
  static Options options;

  public static void main(String[] args) throws Exception {
    options = new Options().setCreateIfMissing(true);
    options.allowMmapReads();
    options.allowMmapWrites();
    db = RocksDB.open(options, dataDir.getPath());

    new LoadDataN().execute();
    System.out.println("All Threads Complete");

    db.close();
    options.dispose();
  }

  class InsertThread extends Thread {

    int size = 0;
    long totalPeriod;
    boolean working = true, showCurrent = false;

    @Override
    public void run() {
      try {

        int currentSize = 0;
        long started = System.currentTimeMillis();
        long startedPortion = started;

        int t = 0;

        while (working) {
          //List<WriteModel<Document>> requests = new ArrayList<>();

          for (int b = 0; b < BULK_SIZE && working; b++) {

            String id = PRE_KEYS[t % USE_KEYS] + t;
            t++;

            String value = RND.str(50);
            db.put(id.getBytes("UTF-8"), value.getBytes("UTF-8"));

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
          }

          //tst1.bulkWrite(requests);

        }

        long totalPeriod = System.currentTimeMillis() - started;

        System.out.println("FINISHED THREAD: " + Thread.currentThread().getName());
        System.out.println(Thread.currentThread().getName() + ": TOTAL Upserted " + size + " records, for "
            + ((double) totalPeriod / 1000.0) + " s; tps = " + tos(((double) size / (double) totalPeriod) * 1000.0));

        this.totalPeriod = totalPeriod;

      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static String getTestId() {
    return RND.str(5);
  }

  private void execute() throws Exception {

    dataDir.mkdirs();

    final InsertThread tt[] = new InsertThread[THREADS_COUNT];
    for (int i = 0; i < THREADS_COUNT; i++) {
      tt[i] = new InsertThread();
    }

    final File signalFile = new File("build/__upload_going_on___");
    signalFile.getParentFile().mkdirs();
    signalFile.createNewFile();

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
  }

  private String tos(double x) {
    return String.format("%4.2f", x);
  }
}
