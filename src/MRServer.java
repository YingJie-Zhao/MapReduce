import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.RMISocketFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Start a RMI server and listen request on localhost:8888
 * Master thread will be initiated at the same time
 *
 * @author YingJie Zhao 2021/6/28
 */
public class MRServer {
    public static void main(String[] args) throws IOException, AlreadyBoundException, InterruptedException {
        LocateRegistry.createRegistry(1099);
        RMISocketFactory.setSocketFactory(new RMISocketFactory() {
            @Override
            public Socket createSocket(String host, int port) throws IOException {
                return new Socket(host, port);
            }

            @Override
            public ServerSocket createServerSocket(int port) throws IOException {
                if (port == 0) {
                    port = 8888;
                }
                System.out.println("RMI port: " + port);
                return new ServerSocket(port);
            }
        });
        Master master = new MasterImpl(readFiles(), 10);
        Naming.bind("rmi://localhost:1099/master", master);
        System.out.println("RMI server started.");
        long begin = System.currentTimeMillis();
        while (true) {
            if (master.done()) {
                long end = System.currentTimeMillis();
                double cost = (double) (end - begin) / 1000;
                System.out.println("RMI server stopped.Total cost: " + "%.2f seconds".formatted(cost));
                // combine mr-out- files into one file,can be used to verify if MapReduce function appropriately executed
                //combine();
                System.exit(0);
            }
            Thread.sleep(2000);
        }
    }

    private static List<String> readFiles() {
        File dir = new File("src/txt");
        File[] files = dir.listFiles();
        if (null == files || files.length < 2) {
            System.out.println("More than 2 input files required!");
            System.exit(1);
        }
        return Arrays.stream(files).map(File::getName).collect(Collectors.toList());
    }

    private static void combine() throws IOException {
        File dir = new File("./");
        File[] files = dir.listFiles();
        if (null == files) {
            System.out.println("No files found!");
            System.exit(1);
        }
        files = Arrays.stream(files).filter(f -> f.getName().startsWith("mr-out-")).toArray(File[]::new);
        List<KeyValue> intermediate = new LinkedList<>();
        for (File file : files) {
            try (FileInputStream fs = new FileInputStream(file)) {
                byte[] data = new byte[(int) file.length()];
                fs.read(data);
                String[] lines = new String(data, StandardCharsets.UTF_8).split("\n");
                for (String line : lines) {
                    String[] content = line.split(" ");
                    intermediate.add(new KeyValue(content[0], content[1]));
                }
            } catch (IOException e) {
                System.out.println("File not found: " + file);
                System.exit(1);
            }
        }

        Collections.sort(intermediate);
        File oFile = new File("combine-out");
        FileWriter fw = new FileWriter(oFile, true);
        for (KeyValue keyValue : intermediate) {
            String content = "%s %s\n".formatted(keyValue.key, keyValue.value);
            fw.append(content);
        }
        fw.close();
    }
}
