package rocksdb;

import kz.greetgo.util.RND;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.File;

public class LoadData1 {

  final File dataDir = new File("/home/pompei/tmp/load_test_rocks_db");
  //final File dataDir = new File("/home/pompei/discs/data2/load_test_rocks_db");

  final static String PRE_KEYS[] = {
      "4678912731942369874568934281764195786345761934856717653736524-",
      "3289749057789456328941695764325693174693281675943259864223777-",
      "4325614875604687596428765348167594150102809850185015856472834-",
  };
  static final int USE_KEYS = 2;

  public static void main(String[] args) throws Exception {
    new LoadData1().execute();
  }

  boolean showCurrent = false;
  boolean working = true;

  static final double GIG = 1_000_000_000.0;


  private void execute() throws Exception {
    System.out.println("Go...");

    RocksDB.loadLibrary();

    final File goingOnFile = new File("build/__going_on__");
    goingOnFile.createNewFile();

    new Thread(new Runnable() {
      @Override
      public void run() {
        try {

          while (true) {

            Thread.sleep(3000);

            if (goingOnFile.exists()) {
              showCurrent = true;
            } else {
              working = false;
              break;
            }

          }

        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }).start();

    Options o = new Options();
    o.allowMmapReads();
    o.allowMmapWrites();
    Options options = o.setCreateIfMissing(true);
    dataDir.mkdirs();
    RocksDB db = RocksDB.open(options, dataDir.getPath());

    try {

      long started = System.nanoTime();

      long last = started;
      int current = 0;
      int all = 0;

      long totalPutTime = 0, currentPutTime = 0;

      int i = 0;

      while (working) {
        String key = PRE_KEYS[i % USE_KEYS] + i;
        i++;
        String value = RND.str(50);

        //if (i > 100) i = 0;

        byte[] keyBytes = key.getBytes("UTF-8");
        byte[] valueBytes = value.getBytes("UTF-8");

        long putTime = System.nanoTime();
        db.put(keyBytes, valueBytes);
        putTime = System.nanoTime() - putTime;

        totalPutTime += putTime;
        currentPutTime += putTime;

        current++;
        all++;

        if (showCurrent) {
          showCurrent = false;
          long now = System.nanoTime();
          long period = now - last;
          last = now;

          int portion = current;
          current = 0;

          double tps = (double) portion / (double) period * GIG;
          double dPeriod = (double) period / GIG;
          double dPutTime = (double) currentPutTime / GIG;
          currentPutTime = 0;

          System.out.println("count/portion = " + all + "/" + portion
              + ", period/putTime = " + tos(dPeriod) + "/" + tos(dPutTime) + " s, ("
              + tos((dPeriod - dPutTime) / dPeriod * 100.0) + " % java), tps = " + tos(tps));
        }
      }

      long finished = System.nanoTime();
      long time = finished - started;

      double tps = (double) all / (double) time * GIG;

      double dTime = (double) time / GIG;
      double dTotalPutTime = (double) totalPutTime / GIG;

      System.out.println("TOTAL: TIME/PUT_TIME = "
          + tos(dTime) + "/" + tos(dTotalPutTime) + " s, (" + tos((dTime - dTotalPutTime) / dTime * 100.0)
          + " % JAVA), COUNT = " + all + ", TPS = " + tos(tps));

    } finally {
      db.close();
      options.dispose();
    }


  }

  private String tos(double x) {
    return String.format("%4.2f", x);
  }
}
