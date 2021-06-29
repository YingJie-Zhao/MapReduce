import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Reduce intermediate files into result files
 *
 * @author YingJie Zhao 2021/06/29
 */
public class ReduceF {
    public static String sequentialReduce(String key, List<String> values) {
        return String.valueOf(values.size());
    }

    public static void reduce(Task task, BiFunction<String, List<String>, String> reduceF) {
        System.out.printf("Reduce worker get task %d\n", task.getTaskID());

        List<KeyValue> intermediate = new ArrayList<>();
        for (int i = 0; i < task.getnMap(); i++) {
            String iName = "mr-%d-%d".formatted(i, task.getTaskID());
            File iFile = new File(iName);
            try (FileInputStream fs = new FileInputStream(iFile)) {
                byte[] data = new byte[(int) iFile.length()];
                fs.read(data);
                String[] lines = new String(data, StandardCharsets.UTF_8).split("\n");
                for (String line : lines) {
                    String[] content = line.split(" ");
                    intermediate.add(new KeyValue(content[0], content[1]));
                }
            } catch (IOException e) {
                System.out.println("File not found: " + iFile);
                System.exit(1);
            }
        }

        Collections.sort(intermediate);
        String oName = "mr-out-%d".formatted(task.getTaskID());
        File oFile = new File(oName);

        int i = 0;
        while (i < intermediate.size()) {
            int j = i + 1;
            while (j < intermediate.size() && intermediate.get(j).key.equals(intermediate.get(i).key)) {
                j++;
            }

            List<String> values = new LinkedList<>();
            for (int k = i; k < j; k++) {
                values.add(intermediate.get(k).value);
            }
            String output = reduceF.apply(intermediate.get(i).key, values);
            try (FileWriter fw = new FileWriter(oFile, true)) {
                String content = "%s %s\n".formatted(intermediate.get(i).key, output);
                fw.append(content);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
            i = j;
        }
    }
}
