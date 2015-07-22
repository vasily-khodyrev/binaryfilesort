package test.org.sw.util;

import org.junit.Test;
import org.sw.util.array.IntByteUtil;

public class TestUtils {
    @Test
    public void testMax2Power() {
         for (int i=1;i<17;i++) {
             int n = IntByteUtil.getMax2Power(i);
             System.out.println("getMax2Power("+i+") = " + n);
         }
    }
}
