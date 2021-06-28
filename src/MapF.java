import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Map files by key hash
 *
 * @author zhaoyingjie 2021/6/28
 */
public class MapF {
    public static List<KeyValue> sequentialMap(String filename, String content) {
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

    public static void map(Task task, BiFunction<String, String, List<KeyValue>> mapF) {
        System.out.printf("Map worker get task %d-%s\n", task.getTaskID(), task.getFileName());

        List<List<KeyValue>> intermediate = new ArrayList<>();
        for (int i = 0; i < task.getnReduce(); i++) {
            intermediate.add(new ArrayList<>());
        }

        File file = new File("src/txt/" + task.getFileName());
        String content = null;
        try (FileInputStream fs = new FileInputStream(file)) {
            byte[] data = new byte[(int) file.length()];
            fs.read(data);
            content = new String(data, StandardCharsets.UTF_8);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        if (content.length() <= 0) {
            System.out.println("Empty file: " + file.getName());
            System.exit(1);
        }

        List<KeyValue> kvl = mapF.apply(task.getFileName(), content);

        for (KeyValue kv : kvl) {
            intermediate.get(hash(kv.key) % task.getnReduce()).add(kv);
        }

        try {
            for (int i = 0; i < task.getnReduce(); i++) {
                if (intermediate.get(i).size() <= 0) {
                    continue;
                }
                String oName = "mr-%d-%d".formatted(task.getTaskID(), i);
                File oFile = new File(oName);
                String template = "%s %s\n";
                FileWriter fw = new FileWriter(oFile, true);
                for (KeyValue kv : intermediate.get(i)) {
                    fw.append(template.formatted(kv.key, kv.value));
                }
                fw.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static int hash(String data) {
        final int p = 16777619;
        int hash = (int) 2166136261L;
        for (int i = 0; i < data.length(); i++) {
            hash = (hash ^ data.charAt(i)) * p;
        }
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        return Math.abs(hash);
    }
}
