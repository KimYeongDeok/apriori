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
    int level = 2;
    String delimiter = ",";

    @Test
    public void asdf() {
        String value = "b,c,d,e";
        String key = "";
        start(key, value);
    }

    private void start(String key, String value) {
        StringTokenizer stringTokenizer = new StringTokenizer(value, delimiter);
        List<String> stringValues = toList(stringTokenizer);

        if (level > stringValues.size())
            return;

        while(stringValues.size() > 0){
            if (1 >= stringValues.size()) {
                String rowKey = createKey(key, stringValues);
                System.out.println(rowKey + "    " + "NULL");
                return;
            }

            String rowKey = createKey(key, stringValues);
            String rowValue = createValue(stringValues);
            System.out.println(rowKey + "    " + rowValue);
        }
    }

    private String createKey(String key, List<String> stringValues) {
        StringBuilder builder = new StringBuilder();

        if ("".equals(key)) {
            builder.append(stringValues.get(0)).append(delimiter);
            stringValues.remove(0);
        }else{
            builder.append(key).append(delimiter).append(stringValues.get(0)).append(delimiter);
            stringValues.remove(0);
        }

        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }

    private String createValue(List<String> stringValues) {
        StringBuilder builder = new StringBuilder();
        for (String stringValue : stringValues) {
            builder.append(stringValue).append(delimiter);
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
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
