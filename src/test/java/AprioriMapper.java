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
    String value = "a,b,c,e,r";
    int level = 1;
    String delimiter = ",";
    String key = "a";

    @Test
    public void start() {
        String tempRow = value;
        int indexSpace = tempRow.indexOf(" ");
        int indexDelimeter = tempRow.indexOf(delimiter);
        int length = Math.abs(indexSpace - indexDelimeter);

        String firstRow = null;
        String secondRow = null;

        if (indexSpace < 0 && length >= 1) {
            indexSpace = 1;
            firstRow = tempRow.substring(0, indexSpace);
            secondRow = tempRow.substring(indexSpace+1, tempRow.length());
        }
        if (indexSpace > 0 && length <= 1) {
            firstRow = tempRow.substring(0, indexSpace);
            secondRow = tempRow.substring(indexSpace+1, tempRow.length());
        }

        StringTokenizer stringTokenizer = new StringTokenizer(secondRow, delimiter);
        List<String> stringValues = toList(stringTokenizer);
        int valueSize = stringValues.size();

        if (level > valueSize)
            return;

        for (int i = 0; i < valueSize; i++) {
            if (1 >= stringValues.size()) {
                String rowKey = createKey(firstRow, stringValues);
                System.out.println(rowKey + "    " + "NULL");
                return;
            }

            String rowKey = createKey(firstRow, stringValues);
            String rowValue = createValue(stringValues);
            System.out.println(rowKey + "    " + rowValue);
        }
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
