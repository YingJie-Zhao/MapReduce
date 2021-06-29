/**
 * Key:word
 * Value:count
 *
 * @author YingJie Zhao 2021/06/29
 */
public class KeyValue implements Comparable<KeyValue> {
    public String key;
    public String value;

    public KeyValue(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int compareTo(KeyValue o) {
        return key.compareTo(o.key);
    }

    @Override
    public String toString() {
        return key + ":" + value;
    }
}
