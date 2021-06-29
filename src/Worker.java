import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Invoke {@link Master#getTask()} to get a task and execute
 * Invoke {@link Master#taskDone(Task)} when task was done
 *
 * @author YingJie Zhao 2021/6/28
 */
public class Worker {

    public void work(BiFunction<String, String, List<KeyValue>> mapF, BiFunction<String, List<String>, String> reduceF) throws MalformedURLException, NotBoundException, RemoteException, InterruptedException {

        Master master = (Master) Naming.lookup("rmi://localhost:1099/master");
        while (true) {
            Task task = master.getTask();
            assert task != null;
            switch (task.getTaskType()) {
                case Task.MAP -> doMap(task, mapF);
                case Task.REDUCE -> doReduce(task, reduceF);
                case Task.WAIT -> {
                    System.out.println("Waiting task...");
                    Thread.sleep(1000);
                    continue;
                }
                case Task.DONE -> {
                    System.out.println("All tasks done");
                    return;
                }
            }
            // Notify master which task was done
            master.taskDone(task);
        }
    }

    private void doMap(Task task, BiFunction<String, String, List<KeyValue>> mapF) {
        MapF.map(task, mapF);
    }

    private void doReduce(Task task, BiFunction<String, List<String>, String> reduceF) {
        ReduceF.reduce(task, reduceF);
    }
}
