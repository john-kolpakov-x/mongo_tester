package mysql;

import kz.greetgo.util.RND;

import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class LoadToMysql {
  public static final int THREADS_COUNT = 10;
  public static final int BATCH_SIZE = 100;

  public static void main(String[] args) throws Exception {
    new LoadToMysql().execute();
    System.out.println("FINISH");
  }

  final static String PRE_KEYS[] = {
      "4678912731942369874568934281764195786345761934856717653736524-",
      "3289749057789456328941611164325693174693281675943259864223777-",
  };
  static int USE_KEYS = 2;

  final AtomicBoolean working = new AtomicBoolean(true);

  private void execute() throws Exception {
    PRE_KEYS[1] = RND.intStr(61) + "-";

    final List<UpsertThread> threadList = new ArrayList<>();
    for (int i = 1; i <= THREADS_COUNT; i++) {
      threadList.add(new UpsertThread(i));
    }


    new Thread() {
      @Override
      public void run() {
        try {

          File file = new File("build/__mysql_is_going_on__");
          file.getParentFile().mkdirs();
          file.createNewFile();

          long started = System.nanoTime();

          while (working.get()) {

            Thread.sleep(3000);

            if (file.exists()) {
              int count = 0;
              for (UpsertThread t : threadList) {
                count += t.count;
              }
              long now = System.nanoTime();
              double period = (double) (now - started) / GIG;
              double tps = (double) count / period;

              System.out.println("-------------------------------- " +
                  "count = " + count + ", period = " + tos(period) + " s, tps = " + tos(tps));

              for (UpsertThread t : threadList) {
                t.showCurrent.set(true);
              }

            } else {
              working.set(false);
            }

          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }.start();

    long started = System.nanoTime();
    for (UpsertThread t : threadList) {
      t.start();
    }
    for (UpsertThread t : threadList) {
      t.join();
    }
    long finished = System.nanoTime();

    int count = 0;
    for (UpsertThread t : threadList) {
      count += t.count;
    }

    double period = (double) (finished - started) / GIG;
    double tps = (double) count / period;

    System.out.println("TOTAL: COUNT = " + count + ", PERIOD = " + tos(period) + " s, TPS = " + tos(tps));
  }

  static final double GIG = 1_000_000_000.0;

  class UpsertThread extends Thread {
    private final int threadNo;
    final AtomicBoolean showCurrent = new AtomicBoolean(false);

    public UpsertThread(int threadNo) {
      this.threadNo = threadNo;
    }

    int count = 0;

    @Override
    public void run() {
      String sql = USE_KEYS == 1 ? "{call upsert_pair(?, ?)}" : "{call upsert_pair_part(?, ?)}";
      int valueSize = USE_KEYS == 1 ? 50 * 15 : 50;

      int portionCount = 0;

      try {
        try (Connection con = MysqlConnection.get()) {

          int u = 0;

          long portionTime = System.nanoTime();

          while (working.get()) {

            try (CallableStatement cs = con.prepareCall(sql)) {
              for (int b = 0; b < BATCH_SIZE; b++) {
                String id = threadNo + "-" + PRE_KEYS[u % USE_KEYS] + u;
                String val = RND.str(valueSize);

                u++;

                cs.setString(1, id);
                cs.setString(2, val);
                cs.addBatch();
                count++;
                portionCount++;

                if (showCurrent.get()) {
                  showCurrent.set(false);
                  long now = System.nanoTime();
                  double period = (double) (now - portionTime) / GIG;
                  portionTime = now;
                  double tps = (double) portionCount / period;
                  System.out.println(Thread.currentThread().getName() + " portion: count = " + portionCount
                      + ", period = " + tos(period) + " s, tps = " + tos(tps));
                  portionCount = 0;
                }
              }
              cs.executeBatch();
            }
          }

        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private String tos(double x) {
    return String.format("%4.2f", x);
  }
}
