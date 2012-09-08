package org.openflamingo.hadoop.mapreduce.apriori;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class TextArrayWriable extends ArrayWritable {
    public TextArrayWriable() {
        super(TextArrayWriable.class);
    }

    public TextArrayWriable(Class<? extends Writable> valueClass) {
        super(TextArrayWriable.class);
    }

    public TextArrayWriable(Text[] texts) {
        super(TextArrayWriable.class, texts);
    }
}
