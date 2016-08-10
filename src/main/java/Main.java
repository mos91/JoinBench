import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.CharArrayReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

public class Main {

  private static String DICTIONARY = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  private static int SORT_BUFFER_SIZE = 131072;

  private static int LINE_SIZE = 26;

  private static int JOINED_LINE_SIZE = 39;

  private static void printHelp(){
    System.out.println("Usage:");
    System.out.println("1. Generate tables : generate-tables 100 1000 input_A.csv input_B.csv");
    System.out.println("2. Join in memory using hash join algorithm (for small tables) : " +
      "join-in-memory input_A.csv input_B.csv output_inmemory.csv");
    System.out.println("3. Join on disk using sort merge algorithm (for big tables and small amount of RAM) : " +
      "join-on-disk input_A.csv input_B.csv outputondisk.csv");
  }

  private static void invalidNumberOfArgs(){
    System.err.println("Invalid number of arguments!");
  }

  private static void invalidArgType(String argName, String neededType){
    System.err.println("Invalid type of " + argName + ", needed " + neededType);
  }

  private static String generateSerialNumber(StringBuilder sb, Random dice){
    char[] buf = new char[14];
    for (int i = 0;i < 14;i++){
      if (i % 5 == 4){
       buf[i] = '-';
      } else {
        buf[i] = DICTIONARY.charAt(dice.nextInt(DICTIONARY.length()));
      }
    }

    return new String(buf).intern();
  }

  private static int getLineKey(char[] line){
    boolean sepFound = false;

    if (line == null || line.length < 25){
      return -1;
    }

    for (char c : line){
      if (c == ','){
        sepFound = true;
        break;
      }
    }

    if (!sepFound){
      return -1;
    }

    int val = 0;
    long p = 1;
    for (int i = 8;i >= 0;i--){
      val += Character.digit(line[i], 10) * p;
      p *= 10;
    }
    return val;
  }

  private static int getLineKey(char[] lines, int offset, int len){
    if (lines == null || offset < 0 || len < 0){
      return -1;
    }

    boolean sepFound = false;
    for (int i = offset * len; i < offset * len  + len;i++){
      char c = lines[i];
      if (c == ','){
        sepFound = true;
        break;
      }
    }

    if (!sepFound){
      return -1;
    }

    int val = 0;
    long p = 1;
    for (int i = offset * len + 8;i >= offset * len;i--){
      val += Character.digit(lines[i], 10) * p;
      p *= 10;
    }

    return val;
  }

  private static char[] buildLine(int key, char[] lineA, char[] lineB, char[] result){
    int val = key;
    int i = 8;
    while (i >= 0){
        int digit = val % 10;
        val /= 10;
        result[i--] = Character.forDigit(digit, 10);
    }

    result[9] = ',';
    i = 23;
    while (i > 9){
      result[i] = lineA[i--];
    }
    result[24] = ',';
    i = 38;
    int j = 23;
    while (i > 24 && j > 9){
      result[i--] = lineB[j--];
    }

    return result;
  }

  private static void doHashJoin(String tablePathA, String tablePathB, String resultPath) {
    final Multimap<String, String> multimap = ArrayListMultimap.create();

    IOUtils.readFrom(tablePathA, (String line) -> {
      String[] parts = line.split(",");
      multimap.put(parts[0], parts[1]);
      return true;
    });

    IOUtils.readFromWriteTo(tablePathB, resultPath, (String line, BufferedWriter writer) -> {
      boolean result = false;
      String[] parts = line.split(",");
      String keyB = parts[0];
      String valueB = parts[1];

      try {
        for (String valueA : multimap.get(keyB)){
          writer.write(keyB + "," + valueA + "," + valueB);
          writer.newLine();
        }

        result = true;
      } catch (IOException e){
        System.err.println("Can not write to file " + resultPath);
        System.exit(-1);
      }

      return result;
    });
  }

  private static void doSortMergeJoin(String tablePathA, String tablePathB, String resultPath) throws IOException {
    char[] lines = new char[SORT_BUFFER_SIZE * LINE_SIZE];
    doSort(tablePathA, lines);
    doSort(tablePathB, lines);
    doMerge(tablePathA, tablePathB, resultPath);
  }

  private static void doMerge(String tablePathA, String tablePathB, String resultPath) {
    boolean isOk = false;
    try (BufferedReader brA = new BufferedReader(new FileReader(tablePathA));
      BufferedReader brB = new BufferedReader(new FileReader(tablePathB));
      BufferedWriter bw = new BufferedWriter(new FileWriter(resultPath))) {

      char[] lineA = new char[LINE_SIZE];
      char[] _lineA = new char[LINE_SIZE];
      int cA = 0;

      char[] lineB = new char[LINE_SIZE];
      char[] _lineB = new char[LINE_SIZE];
      int cB = 0;

      char[] result = new char[JOINED_LINE_SIZE];
      if (brA.ready() && brB.ready()){
        cA = brA.read(lineA);
        cB = brB.read(lineB);
        while(cA != -1 && cB != -1){
          int keyA = getLineKey(lineA);
          int keyB = getLineKey(lineB);

          if (keyA > keyB){
            cB = brB.read(lineB);
          } else if (keyA < keyB){
            cA = brA.read(lineA);

          } else {
            bw.write(buildLine(keyA, lineA, lineB, result));
            bw.newLine();

            brB.mark(8192);
            while ((brB.read(_lineB) != -1) && (getLineKey(_lineB) == keyA)){
              bw.write(buildLine(keyA, lineA, _lineB, result));
              bw.newLine();
            }

            brA.mark(8192);
            while ((brA.read(_lineA) != -1) && (getLineKey(_lineA) == keyB)){
              bw.write(buildLine(keyB, _lineA, lineB, result));
              bw.newLine();
            }

            brA.reset();
            brA.read(lineA);

            brB.reset();
            brB.read(lineB);
          }
        }
      }

      isOk = true;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      System.err.println("Can not be able to locate file");
      isOk = false;
    } catch (IOException e) {
      e.printStackTrace();
      isOk = false;
    } finally {
      if (isOk){
        System.out.println("Sucсessfully merge from " + tablePathA + " and " + tablePathB + " to " + resultPath);
      } else {
        System.err.println("Something goes wrong while merging from " + tablePathA + " and " + tablePathB + ". Closing...");
        System.exit(-1);
      }
    }
  }

  private static void doInitialSegmentation(String tablePath, char[] lines){
    boolean isOk = true;
    try (BufferedReader br = new BufferedReader(new FileReader(tablePath), Math.max(lines.length, SORT_BUFFER_SIZE))){
      if (br.ready()){
        char[] swapBuffer = new char[LINE_SIZE];
        String segmentPath;
        int i = 0;
        int readCount = 0;
        while ((readCount = br.read(lines)) != -1){
          sortLines(lines, 0, (readCount) / LINE_SIZE - 1, swapBuffer);

          segmentPath = tablePath + "." + ++i + ".tmp";
          try (BufferedWriter bw = new BufferedWriter(new FileWriter(segmentPath))) {
            bw.write(lines, 0, readCount);
          } catch (IOException e){
            System.err.println("Could not write to segment " + segmentPath + " .Closing...");
            isOk = false;
          } finally{
            if (isOk){
              System.out.println("Successfully write to temporary segment :" + segmentPath);
            } else {
              System.exit(-1);
            }
          }
        }
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      System.err.println("Could not to open file " + tablePath + " for sort");
      isOk = false;
    } catch (IOException e) {
      e.printStackTrace();
      isOk = false;
    } finally {
      if (isOk){
        System.out.println("Sucessfully dissecting " + tablePath);
      } else {
        System.err.println("Something goes wrong while reading " + tablePath + ". Closing...");
        System.exit(-1);
      }
    }
  }

  private static void doMergeSegments(String tablePath, List<BufferedReader> readers, int level){
    BufferedReader prevReader = null;
    BufferedReader currentReader = null;
    char[] bufferA = new char[LINE_SIZE];
    char[] bufferB = new char[LINE_SIZE];
    int readCountA = 0;
    int readCountB = 0;
    int i = 0;

    List<BufferedReader> newReaders = new ArrayList<>();
    for (BufferedReader reader : readers){
      if (currentReader != null){
        prevReader = currentReader;
      }

      currentReader = reader;

      if (prevReader != null && currentReader != null){
        String segmentPath = tablePath + "." + ++i + ".tmp." + level;

        try(BufferedWriter sinkWriter = new BufferedWriter(new FileWriter(segmentPath))){
          if (prevReader.ready() && currentReader.ready()){
            readCountA = prevReader.read(bufferA);
            readCountB = currentReader.read(bufferB);
            while (readCountA != -1 || readCountB != -1){
              int keyA = getLineKey(bufferA);
              int keyB = getLineKey(bufferB);
              if (readCountA != -1 && readCountB != -1){
                if (keyA <= keyB){
                  sinkWriter.write(bufferA);
                  readCountA = prevReader.read(bufferA);
                } else {
                  sinkWriter.write(bufferB);
                  readCountB = currentReader.read(bufferB);
                }
              } else if (readCountA == -1){
                sinkWriter.write(bufferB);
                readCountB = currentReader.read(bufferB);
              } else if (readCountB == -1){
                sinkWriter.write(bufferA);
                readCountA = prevReader.read(bufferA);
              }
            }
          }

          newReaders.add(Files.newBufferedReader(Paths.get(segmentPath)));
        } catch (IOException e) {
          e.printStackTrace();
        } finally {
          System.out.println("Successfully merge to segment : " + segmentPath + ", level = " + level);
        }

        prevReader = null;
        currentReader = null;
      }
    }

    for (Reader reader : readers){
      try {
        reader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    readers.clear();
    if (!newReaders.isEmpty()){
      readers.addAll(newReaders);
    }

  }

  private static void doSort(String tablePath, char[] lines) throws IOException {
    doInitialSegmentation(tablePath, lines);

    List<BufferedReader> bufferedReaders = new ArrayList<>();
    for (Path path : Files.newDirectoryStream(Paths.get("."), tablePath + ".*.tmp")){
      bufferedReaders.add(Files.newBufferedReader(path));
    }

    int level = 0;
    while (bufferedReaders.size() > 1){
      doMergeSegments(tablePath, bufferedReaders, ++level);
    }

    BufferedReader remainingReader = bufferedReaders.iterator().next();

    char[] buffer = new char[LINE_SIZE];
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(tablePath))){
      while (remainingReader.read(buffer) != -1){
        bw.write(buffer);
      }
    } finally {
      remainingReader.close();
    }

    for (Path path : Files.newDirectoryStream(Paths.get("."), tablePath + ".*{.tmp}")){
      System.out.println("Cleaning temporary file " + path);
      Files.deleteIfExists(path);
    }

    for (Path path : Files.newDirectoryStream(Paths.get("."), tablePath + ".*{.tmp.*}")){
      System.out.println("Cleaning temporary file " + path);
      Files.deleteIfExists(path);
    }
  }

  private static void sortLines(char[] lines, int low, int high, char[] buffer) {
    int i = low;
    int j = high;
    int pivotKey = getLineKey(lines, (i + j) / 2, LINE_SIZE);

    while (i <= j){
      int leftKey = getLineKey(lines, i, LINE_SIZE);
      while (leftKey < pivotKey){
        leftKey = getLineKey(lines, ++i, LINE_SIZE);
      }

      int rightKey = getLineKey(lines, j, LINE_SIZE);
      while (rightKey > pivotKey) {
        rightKey = getLineKey(lines, --j, LINE_SIZE);
      }

      if (i <= j){
        System.arraycopy(lines, i * LINE_SIZE, buffer, 0, LINE_SIZE);
        System.arraycopy(lines, j * LINE_SIZE, lines, i * LINE_SIZE, LINE_SIZE);
        System.arraycopy(buffer, 0, lines, j * LINE_SIZE, LINE_SIZE);
        i++;j--;
      }
    }

    if (low < j){
      sortLines(lines, low, j, buffer);
    }

    if (i < high){
      sortLines(lines, i, high, buffer);
    }

  }

  private static void generateKeys(String tablePath, int rowCount, int keyInterval) {
      boolean isOk = false;

      try (BufferedWriter bw = new BufferedWriter(new FileWriter(tablePath))){
        Random dice = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0;i < rowCount;i++){
          bw.write(String.format("%09d,%s", dice.nextInt(keyInterval), generateSerialNumber(sb, dice)));
          bw.newLine();
        }

        isOk = true;
      } catch (IOException e) {
        System.err.println("File " + tablePath + " can not be open for write!");
      } finally {
        if (isOk){
          System.out.println("Sucessfully wrote to " + tablePath);
        } else {
          System.err.println("Something goes wrong while writing in file " + tablePath + ". Closing...");
          System.exit(-1);
        }
      }
    }

  public static void main(String[] args) {
    if (args.length == 0){
      invalidNumberOfArgs();
      printHelp();

      System.exit(-1);
    }

    String method = args[0];
    if ("generate-tables".equals(method)){
      if (args.length < 5){
        invalidNumberOfArgs();
        printHelp();

        System.exit(-1);
      }

      int rowCount = -1;
      try {
        rowCount = Integer.parseInt(args[1]);
      } catch (NumberFormatException e){
        invalidArgType("rowCount", "integer");
        System.exit(-1);
      }

      int keyInterval = -1;
      try {
        keyInterval = Integer.parseInt(args[2]);
      } catch (NumberFormatException e){
        invalidArgType("keyInterval", "integer");
        System.exit(-1);
      }

      generateKeys(args[3], rowCount, keyInterval);
      generateKeys(args[4], rowCount, keyInterval);
    } else if ("join-in-memory".equals(method)){
      if (args.length < 4){
        invalidNumberOfArgs();
        printHelp();

        System.exit(-1);
      }

      doHashJoin(args[1], args[2], args[3]);
    } else if ("join-on-disk".equals(method)){
      if (args.length < 4){
        invalidNumberOfArgs();
        printHelp();

        System.exit(-1);
      }

      try {
        doSortMergeJoin(args[1], args[2], args[3]);
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      System.out.println("Invalid method specifier!");
      printHelp();

      System.exit(-1);
    }
  }

}
