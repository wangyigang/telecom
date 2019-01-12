import com.wangyg.util.HbaseScanUtil;
import org.junit.Test;

import java.text.ParseException;
import java.util.List;


//Junit 4中 ：非Test开头的不能使用@Test注解，所以比较尴尬
public class TestHbaseUtil {
    @Test
    public void test() throws ParseException {

        List<String[]> arraylist = HbaseScanUtil.getSplitRow("19379884788", "2017-06", "2017-08");
        for (String[] strings : arraylist) {
            System.out.println(strings[0]);
            System.out.println(strings[1]);
            System.out.println("=====================");
        }

    }
}
