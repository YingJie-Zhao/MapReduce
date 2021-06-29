import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Master interface defines RMI function
 *
 * @author YingJie Zhao 2021/6/28
 */
public interface Master extends Remote {

    Task getTask() throws RemoteException;

    void taskDone(Task request) throws RemoteException;

    boolean done() throws RemoteException;
}
