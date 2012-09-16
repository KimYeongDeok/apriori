import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class GenerateFile {
    private String fileName ="test.dat";
    private String text="D";
    private int terms=550;

    @Test
    public void test() throws IOException {
        File file = new File(fileName);
        FileWriter writer = new FileWriter(file, true);
        BufferedWriter bufferedWriter = new BufferedWriter(writer);

        for (int i = 0; i < terms; i++) {
            bufferedWriter.write(text);
            bufferedWriter.write("\r\n");
        }
        bufferedWriter.flush();
        bufferedWriter.close();
    }
}
