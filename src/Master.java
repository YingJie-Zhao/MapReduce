import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Master extends Remote {

    Task getTask() throws RemoteException;

    void taskDone(Task request) throws RemoteException;

    boolean done() throws RemoteException;
}
