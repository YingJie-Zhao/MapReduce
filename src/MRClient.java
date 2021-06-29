import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

/**
 * Start worker thread to get task from master
 *
 * @author YingJie Zhao 2021/6/28
 */
public class MRClient {
    public static void main(String[] args) throws MalformedURLException, NotBoundException, RemoteException, InterruptedException {
        Worker worker = new Worker();
        worker.work(MapF::sequentialMap, ReduceF::sequentialReduce);
    }
}
