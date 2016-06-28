package org.apache.apex.malhar.lib.utils.serde;

import com.datatorrent.netlet.util.Slice;

public class SliceUtils
{
  private SliceUtils()
  {
  }

  public static byte[] concatenate(byte[] a, byte[] b)
  {
    byte[] output = new byte[a.length + b.length];

    System.arraycopy(a, 0, output, 0, a.length);
    System.arraycopy(b, 0, output, a.length, b.length);
    return output;
  }

  public static Slice concatenate(Slice a, Slice b)
  {
    int size = a.length + b.length;
    byte[] bytes = new byte[size];

    System.arraycopy(a.buffer, a.offset, bytes, 0, a.length);
    System.arraycopy(b.buffer, b.offset, bytes, a.length, b.length);

    return new Slice(bytes);
  }

  public static Slice concatenate(byte[] a, Slice b)
  {
    int size = a.length + b.length;
    byte[] bytes = new byte[size];

    System.arraycopy(a, 0, bytes, 0, a.length);
    System.arraycopy(b.buffer, b.offset, bytes, a.length, b.length);

    return new Slice(bytes);
  }

  public static Slice concatenate(Slice a, byte[] b)
  {
    int size = a.length + b.length;
    byte[] bytes = new byte[size];

    System.arraycopy(a.buffer, a.offset, bytes, 0, a.length);
    System.arraycopy(b, 0, bytes, a.length, b.length);

    return new Slice(bytes);
  }
}
