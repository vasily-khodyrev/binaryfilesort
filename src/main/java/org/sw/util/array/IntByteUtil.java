package org.sw.util.array;

/**
 * Provides some utilities to extract integers from byte arrays and transform
 * byte indexes into an integers ones and vise versa
 */
public class IntByteUtil {
    public static long byteIdx(long intIdx) {
        return intIdx << 2;
    }

    public static long intIdx(long byteIdx) {
        return byteIdx >> 2;
    }

    public static int getInt(byte[] a, long index) {
        int ind = (int) byteIdx(index);
        return makeInt(a[ind],a[ind+1],a[ind+2],a[ind+3]);
    }

    public static int makeInt(byte b3, byte b2, byte b1, byte b0) {
        return (int)((((b3 & 0xff) << 24) |
                ((b2 & 0xff) << 16) |
                ((b1 & 0xff) <<  8) |
                ((b0 & 0xff) <<  0)));
    }

    /**
     * Returns nearest power of 2 from above
     * @param n
     * @return
     */
    public static int getMax2Power(int n) {
        int k = 0;
        int temp = n;
        while (temp>0) {
            temp = temp>>1;
            k++;
        }
        int t=1;
        if ((t<<(k-1))<n) k++;
        k--;
        return k;
    }
}