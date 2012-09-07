import com.google.common.hash.Hashing;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class ConsistentHashTest {
    @Test
    public void test(){
        List<String> list = new ArrayList<String>();
        list.add("192.168.10.1");
        list.add("192.168.10.2");
        list.add("192.168.10.3");
        list.add("192.168.10.4");
        list.add("192.168.10.5");
        list.add("192.168.10.6");
        list.add("192.168.10.7");
        list.add("192.168.10.8");

        ConsistentHash<String> map = new ConsistentHash<String>(Hashing.md5(), 8, list);

//        map.remove("0");
//        map.remove("1");
        map.remove("192.168.10.2");
//        for (int i = 0; i < 10; i++) {
//            System.out.println(map.get(""+i));
//        }
        System.out.println(map.get(""));

//        print(map);
//        print(map);
    }

    private void print(ConsistentHash<String> map) {
        for (int i = 0; i < 6; i++) {
            System.out.print(map.get(i + ""));
        }
        System.out.println();
    }

}
