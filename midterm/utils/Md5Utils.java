package utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hbase.util.Bytes;

public class Md5Utils {

  public static final int MD5_LENGTH = 16; // bytes

  public static byte[] md5sum(String s) {
    // This MessageDigest class provides applications the functionality of a message
    // digest algorithm
    // Message digests are secure one-way hash functions that take arbitrary-sized
    // data and output a fixed-length hash value
    MessageDigest d;
    try {
      // Returns a MessageDigest object that implements the specified digest algorithm
      d = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("MD5 algorithm not available!", e);
    }

    // Completes the hash computation
    return d.digest(Bytes.toBytes(s));
  }

}