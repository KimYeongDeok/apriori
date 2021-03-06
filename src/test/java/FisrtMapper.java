import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class FisrtMapper {
    String value = "a,b,c";
    int level = 1;
    String delimiter = ",";
    String key = "a";

    @Test
    public void start() {
        StringTokenizer stringTokenizer = new StringTokenizer(value, delimiter);
        List<String> stringValues = toList(stringTokenizer);

//        String rowKey = createKey(stringValues);

        while (1 != stringValues.size()) {
            String rowKey = createKey(stringValues);
            String rowValue = createValue(stringValues);
            System.out.println(rowKey + "    " + rowValue);
        }
        System.out.println(stringValues.get(0) + "    " + "NULL");
    }

    private String createKey(List<String> stringValues) {
        String rowKey = stringValues.get(0);
        removeFirstValue(stringValues);
        return rowKey;
    }


    private String createValue(List<String> stringValues) {
        StringBuilder builder = new StringBuilder();
        for (String stringValue : stringValues) {
            builder.append(stringValue).append(delimiter);
        }
        builder.delete(builder.length() - 1, builder.length());

        return builder.toString();
    }

    private void removeFirstValue(List<String> stringValues) {
        stringValues.remove(0);
    }

    private List<String> toList(StringTokenizer stringTokenizer) {
        List<String> list = new ArrayList<String>();
        while (stringTokenizer.hasMoreTokens()) {
            String string = stringTokenizer.nextToken();
            if (!"".equals(string.trim()))
                list.add(string);
        }
        return list;
    }
}
