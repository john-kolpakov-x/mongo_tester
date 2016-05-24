package util;

import kz.greetgo.util.RND;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TakeMemory {
  public static void main(String[] args) throws Exception {
    new TakeMemory().execute();
    System.out.println("By-by...");
  }

  final List<byte[]> byteList = new LinkedList<>();
  long totalAllocatedBytes = 0;

  private final static Pattern ALLOC = Pattern.compile("a\\s+(\\d+)\\s+(\\d+)");

  private void execute() throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
    help();

    while (true) {
      System.out.println("<enter command>");
      String line = br.readLine();
      if (line == null) break;

      if ("e".equals(line)) break;
      {
        Matcher m = ALLOC.matcher(line);
        if (m.matches()) {
          alloc(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)));
          continue;
        }
      }

      if ("s".equals(line)) {
        showStatus();
        continue;
      }

      if ("h".equals(line)) {
        help();
        continue;
      }

      if ("f".equals(line)) {
        byteList.clear();
        continue;
      }
    }
  }

  private void help() {
    System.out.println("e - exit");
    System.out.println("h - this help");
    System.out.println("s - status");
    System.out.println("a <count> <size> - allocate <count> blocks of memory with size = <size>");
    System.out.println("f - free memory");
  }

  private void showStatus() {
    System.out.println("totalAllocatedBytes = " + totalAllocatedBytes + ", byteList.size() = " + byteList.size());
  }

  private void alloc(int count, int size) {
    try {
      for (int i = 0; i < count; i++) {
        byte[] bytes = RND.byteArray(size);
        byteList.add(bytes);
        totalAllocatedBytes += bytes.length;
      }
    } catch (OutOfMemoryError e) {
      e.printStackTrace();
    }

    showStatus();
  }

}
