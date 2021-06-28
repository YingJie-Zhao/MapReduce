import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * sequentially map files and reduce into file
 *
 * @author zhaoyingjie 2021/6/24
 */
public class Sequential {
    public static void main(String[] args) throws IOException {
        long begin = System.currentTimeMillis();
        File dir = new File("src/txt");
        File[] files = dir.listFiles();
        if (null == files || files.length < 2) {
            System.out.println("More than 2 input files required!");
            System.exit(1);
        }
        List<KeyValue> intermediate = new ArrayList<>();

        //map files into intermediate variable
        for (File f : files) {
            System.out.println("Mapping " + f.getName());
            try (FileInputStream fs = new FileInputStream(f)) {
                byte[] data = new byte[(int) f.length()];
                fs.read(data);
                String content = new String(data, StandardCharsets.UTF_8);
                List<KeyValue> mapResult = MapF.sequentialMap(f.getName(), content);
                intermediate.addAll(mapResult);
            } catch (FileNotFoundException e) {
                System.out.println("File not found: " + f);
                System.exit(1);
            }
        }

        Collections.sort(intermediate);

        String oName = "mr-out-0.txt";
        String template = "%s %s\n";
        File oFile = new File(oName);
        FileWriter fw = new FileWriter(oFile, true);

        System.out.println("Begin to reduce intermediate data...");
        for (int i = 0; i < intermediate.size(); i++) {
            int j = i + 1;
            while (j < intermediate.size() && intermediate.get(j).key.equals(intermediate.get(i).key)) {
                j++;
            }
            List<String> values = new LinkedList<>();
            for (int k = 0; k < j; k++) {
                values.add(intermediate.get(k).value);
            }

            String output = ReduceF.sequentialReduce(intermediate.get(i).key, values);
            String content = template.formatted(intermediate.get(i).key, output);
            fw.append(content);
            System.out.println(content);

            i = j;
        }
        fw.close();
        long end = System.currentTimeMillis();
        double cost = (double) (end - begin) / (60000);
        System.out.println("Sequential MapReduce cost " + "%.2f".formatted(cost) + "min");
    }
}
