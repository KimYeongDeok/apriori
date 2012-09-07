import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.Iterator;
import java.util.TreeSet;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class AprioriReduce {
    int support = 2;

    @Test
    public void test(){
        Iterator<Text> iterator = null;

        TreeSet<Text> set = new TreeSet<Text>();
        while (iterator.hasNext()) {
            set.add(iterator.next());
        }

        if (set.size() < support)
            return;



        for (Text text : set) {
            if( !"NULL".equals(text))
                System.out.println(text);
        }
    }
}

