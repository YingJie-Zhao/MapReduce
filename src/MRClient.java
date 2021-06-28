import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MRClient {
    public static void main(String[] args) throws MalformedURLException, NotBoundException, RemoteException, InterruptedException {
        ExecutorService executorService = new ThreadPoolExecutor(10, 10, 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        executorService.submit(() -> {
            Worker worker = new Worker();
            try {
                worker.work(MapF::map, ReduceF::reduce);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
