package utils;

import java.io.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import HBaseIA.TwitBase.LoadUsers;

public class LoadUtils {

  // public static final String WORDS_PATH = "/home/xiplus/midterm/sample.csv";
  public static final String WORDS_PATH = "/home/xiplus/midterm/twcs.csv";
  public static final String NAMES_PATH = "/home/xiplus/midterm/propernames";

  public static List<String> readResource(String path) throws IOException {
    // An ordered collection
    // the user of this interface has precise control over where in the list each
    // element is inserted
    // Resizable-array implementation of the List interface
    List<String> lines = new ArrayList<String>();
    String line;
    BufferedReader reader = new BufferedReader(new FileReader(path));
    while ((line = reader.readLine()) != null) {
      lines.add(line);
    }

    reader.close();
    return lines;
  }

  public static int randInt(int max) {
    // The class Math contains methods for performing basic numeric operations
    // Return a double value with a positive sign, greater than or equal to 0.0 and
    // less than 1.0
    // Return the largest double value that is less than or equal to the argument
    return (int) Math.floor(Math.random() * max);
  }

  public static String randNth(List<String> words) {
    int val = randInt(words.size());
    // Return the element at the specified position in this list
    return words.get(val);
  }
}
