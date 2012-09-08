import com.google.common.base.Splitter;
import org.junit.Test;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class asdfsdf {
    @Test
    public void asdf(){
        Iterable<String> split = Splitter.on(',').trimResults().split("123,2,32,3,,23, ,23,");
        for (String s : split) {
            System.out.print(s);
        }
        System.out.println("a");
    }
}
