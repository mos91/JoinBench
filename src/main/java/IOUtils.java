import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;

public class IOUtils {

  public static void readFrom(String tablePath, Function<String, Boolean> lineProcessor){
    boolean isOk = false;
    try (BufferedReader br = new BufferedReader(new FileReader(tablePath))){
      String line;
      if (br.ready()){
        while ((line = br.readLine()) != null){
          if (!lineProcessor.apply(line)) break;
        }

        if (line == null){
          isOk = true;
        }
      }

      isOk = true;
    } catch (FileNotFoundException e) {
      System.err.println("File " + tablePath + " not found!");
      isOk = false;
    } catch (OutOfMemoryError e){
      System.err.println("Program has run out of memory!");
      isOk = false;
    } catch (IOException e) {
      System.err.println("File " + tablePath + " can not be opened for reading!");
      isOk = false;
    } finally {
      if (isOk){
        System.out.println("Sucessfully read from " + tablePath);
      } else {
        System.err.println("Something goes wrong while reading from " + tablePath + ". Closing...");
        System.exit(-1);
      }
    }
  }

  public static void readFromWriteTo(String tablePath, String resultPath,
    BiFunction<String, BufferedWriter, Boolean> lineProcessor){
    boolean isOk = false;
    try (BufferedReader br = new BufferedReader(new FileReader(tablePath))){
      String line;
      if (br.ready()){
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(resultPath))){
          while ((line = br.readLine()) != null){
            if (!lineProcessor.apply(line, bw)) break;
          }

          if (line == null){
            isOk = true;
          }
        } catch (IOException e){
          System.err.println("File " + resultPath + " can not be opened for writing!");
          isOk = false;
        }
      }
    } catch (FileNotFoundException e) {
      System.err.println("File " + tablePath + " not found!");
      isOk = false;
    } catch (IOException e) {
      System.err.println("File " + tablePath + " can not be opened for reading!");
      isOk = false;
    } finally {
      if (isOk){
        System.out.println("Sucessfully read from " + tablePath + ". Successfully write to " + resultPath);
      } else {
        System.err.println("Something goes wrong while reading from " + tablePath +
          " and writing to " + resultPath + ". Closing...");
        System.exit(-1);
      }
    }
  }
}
