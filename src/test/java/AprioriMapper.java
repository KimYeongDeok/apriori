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
public class AprioriMapper {
    String value = "3\t4,5";
    int level = 2;
    String delimiter = ",";
    String key = "a";

    @Test
    public void start() {
        String tempRow = value;
        int indexSpace = tempRow.indexOf("\t");

        String firstRow = tempRow.substring(0, indexSpace);
        String secondRow = tempRow.substring(indexSpace+1, tempRow.length());

        StringTokenizer stringTokenizer = new StringTokenizer(secondRow, delimiter);
        List<String> stringValues = toList(stringTokenizer);
        int valueSize = stringValues.size();

        while (1 != stringValues.size()) {
            String rowKey = createKey(firstRow, stringValues);
            String rowValue = createValue(stringValues);
            System.out.println(rowKey + "    " + rowValue);
        }
        String rowKey = createKey(firstRow, stringValues);
        System.out.println(rowKey + "    " + "NULL");
    }


    private String createKey(String key, List<String> stringValues) {
        key = key + delimiter + stringValues.get(0);
        stringValues.remove(0);

        return key;
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
