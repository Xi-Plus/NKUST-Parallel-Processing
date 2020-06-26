package HBaseIA.TwitBase.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import utils.Md5Utils;

public class TwitsDAO {

  public static final byte[] TABLE_NAME = Bytes.toBytes("twits");
  public static final byte[] TWITS_FAM = Bytes.toBytes("twits");

  public static final byte[] USER_COL = Bytes.toBytes("user");
  public static final byte[] TWIT_COL = Bytes.toBytes("twit");
  private static final int longLength = 8; // bytes

  // A simple pool of HTable instances
  private HTablePool pool;

  // Most logging operations, except configuration, are done through this class
  // Retrieve a logger named according to the value of the name parameter
  private static final Logger log = Logger.getLogger(TwitsDAO.class);

  public TwitsDAO(HTablePool pool) {
    this.pool = pool;
  }

  private static byte[] mkRowKey(Twit t) {
    return mkRowKey(t.user, t.dt);
  }

  private static byte[] mkRowKey(String user, DateTime dt) {
    byte[] userHash = Md5Utils.md5sum(user);
    // Gets the milliseconds of the datetime instant from the Java epoch of
    // 1970-01-01T00:00:00Z
    byte[] timestamp = Bytes.toBytes(-1 * dt.getMillis());
    byte[] rowKey = new byte[Md5Utils.MD5_LENGTH + longLength];

    int offset = 0;
    // Put bytes at the specified byte array position
    // tgtBytes - the byte array; tgtOffset - position in the array; srcBytes -
    // array to write out; srcOffset - source offset; srcLength - source length
    // Return incremented offset
    offset = Bytes.putBytes(rowKey, offset, userHash, 0, userHash.length);
    Bytes.putBytes(rowKey, offset, timestamp, 0, timestamp.length);
    return rowKey;
  }

  private static Put mkPut(Twit t) {
    // Used to perform Put operations for a single row
    Put p = new Put(mkRowKey(t));
    // Add the specified column and value to this Put operation
    p.add(TWITS_FAM, USER_COL, Bytes.toBytes(t.user));
    p.add(TWITS_FAM, TWIT_COL, Bytes.toBytes(t.text));
    return p;
  }

  private static Get mkGet(String user, DateTime dt) {
    // Used to perform Get operations on a single row
    Get g = new Get(mkRowKey(user, dt));
    // Get the column from the specific family with the specified qualifier
    g.addColumn(TWITS_FAM, USER_COL);
    g.addColumn(TWITS_FAM, TWIT_COL);
    return g;
  }

  private static String to_str(byte[] xs) {
    // A mutable sequence of characters
    // Constructs a string builder with no characters in it and an initial capacity
    // specified by the capacity argument
    // This class is designed for use as a drop-in replacement for StringBuffer in
    // places where the string buffer was being used by a single thread
    StringBuilder sb = new StringBuilder(xs.length * 2);
    for (byte b : xs) {
      // Append the string representation of the char argument to this sequence
      sb.append(b).append(" ");
    }
    // Remove the char at the specified position in this sequence
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  private static Scan mkScan(String user) {
    byte[] userHash = Md5Utils.md5sum(user);
    // Pad zeros at the end of source byte array
    byte[] startRow = Bytes.padTail(userHash, longLength);
    byte[] stopRow = Bytes.padTail(userHash, longLength);
    stopRow[Md5Utils.MD5_LENGTH - 1]++;

    // The DEBUG Level designates fine-grained informational events that are most
    // useful to debug an application
    // Log a message object with the DEBUG level
    log.debug("Scan starting at: '" + to_str(startRow) + "'");
    log.debug("Scan stopping at: '" + to_str(stopRow) + "'");

    // Used to perform Scan operations
    // Create a Scan operation for the range of rows specified
    // startRow - row to start scanner at or after; inclusive
    // stopRow - row to stop scanner before; exclusive
    Scan s = new Scan(startRow, stopRow);
    // Get the column from the specified family with the specified qualifier
    s.addColumn(TWITS_FAM, USER_COL);
    s.addColumn(TWITS_FAM, TWIT_COL);
    return s;
  }

  public void postTwit(String user, DateTime dt, String text) throws IOException {
    // Used to communicate with a single HBase table
    // Get a reference to the specified table from the pool
    // Create a new one if the specified table is not available
    HTableInterface twits = pool.getTable(TABLE_NAME);

    Put p = mkPut(new Twit(user, dt, text));
    // Put some data in the table in batch
    twits.put(p);

    // Release any resources held or pending changes in internal buffers
    twits.close();
  }

  public HBaseIA.TwitBase.model.Twit getTwit(String user, DateTime dt) throws IOException {
    // Used to communicate with a single HBase table
    // Get a reference to the specified table from the pool
    // Create a new one if the specified table is not available
    HTableInterface twits = pool.getTable(TABLE_NAME);

    Get g = mkGet(user, dt);
    // Single row result of a Get or Scan query
    // Extract certain cells from a given row
    Result result = twits.get(g);
    if (result.isEmpty())
      return null;

    Twit t = new Twit(result);
    // Release any resources held or pending changes in internal buffers
    twits.close();
    return t;
  }

  public List<HBaseIA.TwitBase.model.Twit> list(String user) throws IOException {
    // Used to communicate with a single HBase table
    // Get a reference to the specified table from the pool
    // Create a new one if the specified table is not available
    HTableInterface twits = pool.getTable(TABLE_NAME);

    // Interface for client-side scanning
    // Go to Table to obtain instances
    // Return a scanner on the current table as specified by the Scan object
    ResultScanner results = twits.getScanner(mkScan(user));
    // An ordered collection
    // the user of this interface has precise control over where in the list each
    // element is inserted
    // Resizable-array implementation of the List interface
    List<HBaseIA.TwitBase.model.Twit> ret = new ArrayList<HBaseIA.TwitBase.model.Twit>();
    // Single row result of a Get or Scan query
    for (Result r : results) {
      // Append the specified element to the end of this list
      ret.add(new Twit(r));
    }

    // Release any resources held or pending changes in internal buffers
    twits.close();
    return ret;
  }

  private static class Twit extends HBaseIA.TwitBase.model.Twit {

    private Twit(Result r) {
      // Copy the specified range of the specified array into a new array
      // from - the initial index of the range to be copied, inclusive; to - the final
      // index of the range to be copied, exclusive
      // The Cell for the most recent timestamp for a given column
      // Method for retrieving the row key that corresponds to the row from which this
      // Result was created
      this(r.getColumnLatest(TWITS_FAM, USER_COL).getValue(),
          Arrays.copyOfRange(r.getRow(), Md5Utils.MD5_LENGTH, Md5Utils.MD5_LENGTH + longLength),
          r.getColumnLatest(TWITS_FAM, TWIT_COL).getValue());
    }

    private Twit(byte[] user, byte[] dt, byte[] text) {
      this(Bytes.toString(user), new DateTime(-1 * Bytes.toLong(dt)), Bytes.toString(text));
    }

    private Twit(String user, DateTime dt, String text) {
      this.user = user;
      this.dt = dt;
      this.text = text;
    }
  }
}