import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * sequentially map files and reduce into file
 *
 * @author zhaoyingjie 2021/6/24
 */
public class Sequential {
    public static void main(String[] args) throws IOException {
        File dir = new File("/Users/zhaoyingjie/IdeaProjects/MapReduce/src/txt");
        File[] files = dir.listFiles();
        if (null == files || files.length < 2) {
            System.out.println("More than 2 input files required!");
            System.exit(1);
        }
        List<KeyValue> intermediate = new ArrayList<>();

        //map files into intermediate variable
        for (File f : files) {
            try (FileInputStream fs = new FileInputStream(f)) {
                byte[] data = new byte[(int) f.length()];
                fs.read(data);
                String content = new String(data, StandardCharsets.UTF_8);
                List<KeyValue> mapResult = MapF.map(f.getName(), content);
                intermediate.addAll(mapResult);
            } catch (FileNotFoundException e) {
                System.out.println("File not found: " + f);
                System.exit(1);
            }
        }
    }
}
