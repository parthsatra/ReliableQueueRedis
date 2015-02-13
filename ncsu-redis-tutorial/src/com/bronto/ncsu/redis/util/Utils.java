package com.bronto.ncsu.redis.util;

import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;

import java.util.List;
import java.util.Random;

public final class Utils {
  private static final Random RANDOM = new Random();
  private static final BaseEncoding ENCODING = BaseEncoding.base16().lowerCase();

  private Utils() {}

  /**
   * Return a random array of bytes of the given length.
   *
   * @param n
   * @return
   */
  public static byte[] randomBytes(int n) {
    byte[] bytes = new byte[n];
    RANDOM.nextBytes(bytes);
    return bytes;
  }

  /**
   * Return a random array of bytes encoded as a base 16 string. Since n bytes
   * are being generated the string returned will be 2n characters long.
   *
   * @param n
   * @return
   */
  public static String randomBytesString(int n) {
    return ENCODING.encode(randomBytes(n));
  }

  /**
   * Generate m random byte strings as in randomBytesString;
   *
   * @param n
   * @param m
   * @return
   */
  public static List<String> randomByteStrings(int n, int m) {
    List<String> strings = Lists.newArrayList();
    for (int i = 0; i < m; i++) {
      strings.add(randomBytesString(n));
    }
    return strings;
  }

  /**
   * Return the current time in milliseconds since the epoch of
   * Jan 1 1970. See {@link System#currentTimeMillis()}.
   *
   * @return
   */
  public static long currentTimeMillis() {
    return System.currentTimeMillis();
  }

}
