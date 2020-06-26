package HBaseIA.TwitBase.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class UsersDAO {

  public static final byte[] TABLE_NAME = Bytes.toBytes("users");
  public static final byte[] INFO_FAM = Bytes.toBytes("info");

  public static final byte[] USER_COL = Bytes.toBytes("user"); // the row key
  public static final byte[] NAME_COL = Bytes.toBytes("name");
  public static final byte[] EMAIL_COL = Bytes.toBytes("email");
  public static final byte[] PASS_COL = Bytes.toBytes("password");
  public static final byte[] TWEETS_COL = Bytes.toBytes("tweet_count");

  public static final byte[] HAMLET_COL = Bytes.toBytes("hamlet_tag");

  // Most logging operations, except configuration, are done through this class
  // Retrieve a logger named according to the name parameter
  // If the named logger already exists, then the existing instance will be
  // returned
  // Otherwise, a new instance is created
  private static final Logger log = Logger.getLogger(UsersDAO.class);

  // A simple pool of HTable instances. Each HTablePool acts as a pool for all
  // tables
  private HTablePool pool;

  public UsersDAO(HTablePool pool) {
    this.pool = pool;
  }

  // produce a Get instance
  private static Get mkGet(String user) throws IOException {
    // The DEBUG Level designates fine-grained informational events that are most
    // useful to debug an application
    // Log a message object with the DEBUG level
    log.debug(String.format("Creating Get for %s", user));

    // Used to perform Get operations on a single row
    Get g = new Get(Bytes.toBytes(user));
    // Get all columns from the specified family
    g.addFamily(INFO_FAM);
    return g;
  }

  // produce a Put instance
  private static Put mkPut(User u) {
    // The DEBUG Level designates fine-grained informational events that are most
    // useful to debug an application
    // Log a message object with the DEBUG level
    log.debug(String.format("Creating Put for %s", u));

    // Used to perform Put operations for a single row
    Put p = new Put(Bytes.toBytes(u.user));
    // Add the specified column and value to this Put operation
    p.add(INFO_FAM, USER_COL, Bytes.toBytes(u.user));
    p.add(INFO_FAM, NAME_COL, Bytes.toBytes(u.name));
    p.add(INFO_FAM, EMAIL_COL, Bytes.toBytes(u.email));
    p.add(INFO_FAM, PASS_COL, Bytes.toBytes(u.password));
    return p;
  }

  // produce a Put instance
  public static Put mkPut(String username, byte[] fam, byte[] qual, byte[] val) {
    // Used to perform Put operations for a single row
    Put p = new Put(Bytes.toBytes(username));
    // Add the specified column and value to this Put operation
    p.add(fam, qual, val);
    return p;
  }

  // produce a Delete instance
  private static Delete mkDel(String user) {
    // The DEBUG Level designates fine-grained informational events that are most
    // useful to debug an application
    // Log a message object with the DEBUG level
    log.debug(String.format("Creating Delete for %s", user));

    // Used to perform Delete operations on a single row
    Delete d = new Delete(Bytes.toBytes(user));
    return d;
  }

  // produce a Scan instance
  private static Scan mkScan() {
    // Used to perform Scan operations
    Scan s = new Scan();
    // Get all columns from the specified family
    s.addFamily(INFO_FAM);
    return s;
  }

  // insert one row record
  public void addUser(String user, String name, String email, String password) throws IOException {

    // Used to communicate with a single HBase table
    // Get a reference to the specified table from the pool
    // Create a new one if the specified table is not available
    HTableInterface users = pool.getTable(TABLE_NAME);

    Put p = mkPut(new User(user, name, email, password));
    // Put some data in the table in batch
    users.put(p);

    // Release any resources held or pending changes in internal buffers
    users.close();
  }

  // get one row record
  public HBaseIA.TwitBase.model.User getUser(String user) throws IOException {
    // Used to communicate with a single HBase table
    // Get a reference to the specified table from the pool
    // Create a new one if the specified table is not available
    HTableInterface users = pool.getTable(TABLE_NAME);

    Get g = mkGet(user);
    // Single row result of a Get or Scan query
    // Extract certain cells from a given row
    Result result = users.get(g);
    if (result.isEmpty()) {
      // The INFO level designates informational messages that highlight the progress
      // of the application at coarse-grained level
      // Log a message object with the INFO Level
      log.info(String.format("user %s not found.", user));
      return null;
    }

    User u = new User(result);
    // Release any resources held or pending changes in internal buffers
    users.close();
    return u;
  }

  // delete one row record
  public void deleteUser(String user) throws IOException {
    // Used to communicate with a single HBase table
    // Get a reference to the specified table from the pool
    // Create a new one if the specified table is not available
    HTableInterface users = pool.getTable(TABLE_NAME);

    Delete d = mkDel(user);
    // Deletes the specified cells or rows in bulk
    users.delete(d);

    // Release any resources held or pending changes in internal buffers
    users.close();
  }

  // list all row records of the table
  public List<HBaseIA.TwitBase.model.User> getUsers() throws IOException {
    // Used to communicate with a single HBase table
    // Get a reference to the specified table from the pool
    // Create a new one if the specified table is not available
    HTableInterface users = pool.getTable(TABLE_NAME);

    // Interface for client-side scanning
    // Go to Table to obtain instances
    // Return a scanner on the current table as specified by the Scan object
    ResultScanner results = users.getScanner(mkScan());
    // Resizable-array implementation of the List interface
    ArrayList<HBaseIA.TwitBase.model.User> ret = new ArrayList<HBaseIA.TwitBase.model.User>();
    // Single row result of a Get or Scan query
    for (Result r : results) {
      // Append the specified element to the end of this list
      ret.add(new User(r));
    }

    // Release any resources held or pending changes in internal buffers
    users.close();
    return ret;
  }

  // increase the tweet count for one row record
  public long incTweetCount(String user) throws IOException {
    // Used to communicate with a single HBase table
    // Get a reference to the specified table from the pool
    // Create a new one if the specified table is not available
    HTableInterface users = pool.getTable(TABLE_NAME);

    // Atomically increments a column value
    long ret = users.incrementColumnValue(Bytes.toBytes(user), INFO_FAM, TWEETS_COL, 1L);

    // Release any resources held or pending changes in internal buffers
    users.close();
    return ret;
  }

  private static class User extends HBaseIA.TwitBase.model.User {
    private User(Result r) {
      // Get the latest version of the specified column
      this(r.getValue(INFO_FAM, USER_COL), r.getValue(INFO_FAM, NAME_COL), r.getValue(INFO_FAM, EMAIL_COL),
          r.getValue(INFO_FAM, PASS_COL),
          r.getValue(INFO_FAM, TWEETS_COL) == null ? Bytes.toBytes(0L) : r.getValue(INFO_FAM, TWEETS_COL));
    }

    private User(byte[] user, byte[] name, byte[] email, byte[] password, byte[] tweetCount) {
      this(Bytes.toString(user), Bytes.toString(name), Bytes.toString(email), Bytes.toString(password));
      this.tweetCount = Bytes.toLong(tweetCount);
    }

    private User(String user, String name, String email, String password) {
      this.user = user;
      this.name = name;
      this.email = email;
      this.password = password;
    }
  }
}
