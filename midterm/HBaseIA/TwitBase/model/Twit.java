package HBaseIA.TwitBase.model;

import org.joda.time.DateTime;

public abstract class Twit {

  public int tweet_id;
  public String text;

  @Override
  public String toString() {
    return String.format("<Twit: %d %s>", tweet_id, text);
  }
}
