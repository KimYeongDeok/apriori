import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class asdfsdf {
    @Test
    public void a(){
        Map<String, String> map = new HashMap<String, String>();

        String delimiter="::";
        String timeStamp ="a";
        String movie="b";
        StringBuilder movies = new StringBuilder();
        if(map.containsKey(timeStamp)){
            movies.append(delimiter).append(map.get(timeStamp));
        }else{
            movies.append(movie);
        }
        map.put(timeStamp, movies.toString());

        delimiter="::";
        timeStamp ="a";
        movie="c";
        StringBuilder movies2 = new StringBuilder();
        if (map.containsKey(timeStamp)) {
            movies2.append(map.get(timeStamp)).append(delimiter).append(movie);
        } else {
            movies2.append(movie);
        }
        System.out.println(movies2);
        map.put(timeStamp, movies2.toString());
    }
}
