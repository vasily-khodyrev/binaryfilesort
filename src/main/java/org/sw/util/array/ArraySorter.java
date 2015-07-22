package org.sw.util.array;

/**
 * Utility to sort integers inside byte array.
 * Just an adaptation of standard JDK Arrays.sort algorithm.
 */
public class ArraySorter {

    public static void sortBigEndianInts(byte[] a, int pos, int length) {
          rangeCheck(a.length,pos, pos+length-1);
          sort1(a, pos, length);
    }

    /**
     * Check that fromIndex and toIndex are in range, and throw an
     * appropriate exception if they aren't.
     */
    private static void rangeCheck(int arrayLen, int fromIndex, int toIndex) {
        if (fromIndex > toIndex)
            throw new IllegalArgumentException("fromIndex(" + fromIndex +
                    ") > toIndex(" + toIndex+")");
        if (fromIndex < 0)
            throw new ArrayIndexOutOfBoundsException(fromIndex);
        if (toIndex > arrayLen)
            throw new ArrayIndexOutOfBoundsException(toIndex);
    }

    /**
     * Sorts the specified sub-array of integers stored in byte array into ascending order.
     */
    private static void sort1(byte x[], int off, int len) {
        // Insertion sort on smallest array
        if (len < 7) {
            for (int i=off; i<len+off; i++)
                for (int j=i; j>off && IntByteUtil.getInt(x,j-1)>IntByteUtil.getInt(x,j); j--)
                    swap(x, j, j-1);
            return;
        }

        // Choose a partition element, v
        int m = off + (len >> 1);       // Small array, middle element
        if (len > 7) {
            int l = off;
            int n = off + len - 1;
            if (len > 40) {        // Big array, pseudomedian of 9
                int s = len/8;
                l = med3(x, l,     l+s, l+2*s);
                m = med3(x, m-s,   m,   m+s);
                n = med3(x, n-2*s, n-s, n);
            }
            m = med3(x, l, m, n); // Mid-size, med of 3
        }

        int v = IntByteUtil.getInt(x,m);

        // Establish Invariant: v* (<v)* (>v)* v*
        int a = off, b = a, c = off + len - 1, d = c;
        while(true) {
            int xbcv = IntByteUtil.getInt(x,b);
            while (b <= c && xbcv <= v) {
                if (xbcv == v)
                    swap(x, a++, b);
                b++;
                xbcv = IntByteUtil.getInt(x,b);
            }
            xbcv = IntByteUtil.getInt(x,c);
            while (c >= b && xbcv >= v) {
                if (xbcv == v)
                    swap(x, c, d--);
                c--;
                xbcv = IntByteUtil.getInt(x,c);
            }
            if (b > c)
                break;
            swap(x, b++, c--);
        }

        // Swap partition elements back to middle
        int s, n = off + len;
        s = Math.min(a-off, b-a  );  vecswap(x, off, b-s, s);
        s = Math.min(d-c,   n-d-1);  vecswap(x, b,   n-s, s);

        // Recursively sort non-partition-elements
        if ((s = b-a) > 1)
            sort1(x, off, s);
        if ((s = d-c) > 1)
            sort1(x, n-s, s);
    }

    /**
     * Swaps x[a] with x[b].
     */
    private static void swap(byte x[], int a, int b) {
        int ai = (int) IntByteUtil.byteIdx(a);
        int bi = (int) IntByteUtil.byteIdx(b);
        byte t = x[ai];
        byte t1 = x[ai+1];
        byte t2 = x[ai+2];
        byte t3 = x[ai+3];
        x[ai] = x[bi];
        x[ai+1] = x[bi+1];
        x[ai+2] = x[bi+2];
        x[ai+3] = x[bi+3];
        x[bi] = t;
        x[bi+1] = t1;
        x[bi+2] = t2;
        x[bi+3] = t3;
    }

    /**
     * Swaps x[a .. (a+n-1)] with x[b .. (b+n-1)].
     */
    private static void vecswap(byte x[], int a, int b, int n) {
        for (int i=0; i<n; i++, a++, b++)
            swap(x, a, b);
    }

    /**
     * Returns the index of the median of the three indexed integers.
     */
    private static int med3(byte x[], int a, int b, int c) {
        int av = IntByteUtil.getInt(x,a);
        int bv = IntByteUtil.getInt(x,b);
        int cv = IntByteUtil.getInt(x,c);
        return (av < bv ?
                (bv < cv ? b : av < cv ? c : a) :
                (bv > cv ? b : av > cv ? c : a));
    }
}
