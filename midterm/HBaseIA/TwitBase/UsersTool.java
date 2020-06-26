package HBaseIA.TwitBase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.log4j.Logger;

import HBaseIA.TwitBase.hbase.UsersDAO;
import HBaseIA.TwitBase.model.User;

public class UsersTool {

  // Most logging operations, except configuration, are done through this class
  // Retrieve a logger named according to the name parameter
  // If the named logger already exists, then the existing instance will be
  // returned
  // Otherwise, a new instance is created
  private static final Logger log = Logger.getLogger(UsersTool.class);

  public static final String usage = "usertool action ...\n" + "  help - print this message and exit.\n"
      + "  add user name email password - add a new user.\n" + "  get user - retrieve a specific user.\n"
      + "  list - list all installed users.\n";

  public static void main(String[] args) throws IOException {
    if (args.length == 0 || "help".equals(args[0])) {
      System.out.println(usage);
      System.exit(0);
    }

    // A simple pool of HTable instances
    // Each HTablePool acts as a pool for all tables
    HTablePool pool = new HTablePool();
    UsersDAO dao = new UsersDAO(pool);

    if ("get".equals(args[0])) {
      // The DEBUG Level designates fine-grained informational events that are most
      // useful to debug an application
      // Log a message object with the DEBUG level
      log.debug(String.format("Getting user %s", args[1]));
      User u = dao.getUser(args[1]);
      System.out.println(u);
    }

    if ("add".equals(args[0])) {
      // The DEBUG Level designates fine-grained informational events that are most
      // useful to debug an application
      // Log a message object with the DEBUG level
      log.debug("Adding user...");
      dao.addUser(args[1], args[2], args[3], args[4]);
      User u = dao.getUser(args[1]);
      System.out.println("Successfully added user " + u);
    }

    if ("list".equals(args[0])) {
      // An ordered collection
      // the user of this interface has precise control over where in the list each
      // element is inserted
      List<User> users = dao.getUsers();
      // The INFO level designates informational messages that highlight the progress
      // of the application at coarse-grained level
      // Log a message object with the INFO Level
      log.info(String.format("Found %s users.", users.size()));
      for (User u : users) {
        System.out.println(u);
      }
    }

    // Closes all the HTable instances , belonging to the given table, in the table
    // pool
    pool.closeTablePool(UsersDAO.TABLE_NAME);
  }
}
