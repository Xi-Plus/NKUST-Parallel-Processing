package HBaseIA.TwitBase.model;

//abstract class for one row record
public abstract class User {

  public String user; // the row key
  public String name;
  public String email;
  public String password;
  public long tweetCount;

  @Override
  public String toString() {
    return String.format("<User: %s, %s, %s, %s>", user, name, email, tweetCount);
  }
}
