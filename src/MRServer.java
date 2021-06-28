import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.RMISocketFactory;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
        while (true) {
            if (master.done()) {
                System.out.println("RMI server stopped.");
                System.exit(0);
            }
            Thread.sleep(10000);
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
}
