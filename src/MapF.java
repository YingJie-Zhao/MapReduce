import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MapF {
    public static List<KeyValue> map(String filename, String content) {
        Pattern pattern = Pattern.compile("[a-zA-Z]+");
        Matcher matcher = pattern.matcher(content);
        List<String> words = new ArrayList<>();
        while (matcher.find()) {
            words.add(matcher.group());
        }
        List<KeyValue> result = new ArrayList<>();
        for (String word : words) {
            KeyValue kv = new KeyValue(word, "1");
            result.add(kv);
        }
        return result;
    }
}
