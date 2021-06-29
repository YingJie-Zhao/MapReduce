import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

/**
 * Dispatch tasks to worker
 *
 * @author YingJie Zhao 2021/6/28
 */
public class MasterImpl extends UnicastRemoteObject implements Master {

    private final Map<Integer, Task> mapTasksReady;
    private final Map<Integer, Task> mapTasksInProgress;

    private final Map<Integer, Task> reduceTasksReady;
    private final Map<Integer, Task> reduceTasksInProgress;

    private boolean reduceReady;
    private final int nReduce;
    private final int nMap;

    public MasterImpl(List<String> files, int nReduce) throws RemoteException {
        mapTasksReady = new HashMap<>(16);
        mapTasksInProgress = new HashMap<>(16);
        reduceTasksReady = new HashMap<>(16);
        reduceTasksInProgress = new HashMap<>(16);

        for (int i = 0; i < files.size(); i++) {
            mapTasksReady.put(i, new Task(files.get(i), Task.MAP, i, nReduce, files.size(), System.currentTimeMillis()));
        }

        reduceReady = false;
        this.nReduce = nReduce;
        nMap = files.size();
    }

    private void collectStallTasks() {
        //collect when task overrun 1s
        long curTime = System.currentTimeMillis();
        Set<Map.Entry<Integer, Task>> mEntrySet = mapTasksInProgress.entrySet();
        Iterator<Map.Entry<Integer, Task>> mIterator = mEntrySet.iterator();
        while (mIterator.hasNext()) {
            Map.Entry<Integer, Task> entry = mIterator.next();
            if (curTime - entry.getValue().getTimeStamp() > 10000) {
                mapTasksReady.put(entry.getKey(), entry.getValue());
                mIterator.remove();
                System.out.printf("Collect map task %d\n", entry.getKey());
            }
        }
        Set<Map.Entry<Integer, Task>> nEntrySet = reduceTasksInProgress.entrySet();
        Iterator<Map.Entry<Integer, Task>> nIterator = nEntrySet.iterator();
        while (nIterator.hasNext()) {
            Map.Entry<Integer, Task> entry = nIterator.next();
            if (curTime - entry.getValue().getTimeStamp() > 10000) {
                reduceTasksReady.put(entry.getKey(), entry.getValue());
                nIterator.remove();
                System.out.printf("Collect reduce task %d\n", entry.getKey());
            }
        }
    }

    @Override
    public Task getTask() throws RemoteException {
        synchronized (this) {
            collectStallTasks();

            Task response = new Task();
            if (mapTasksReady.size() > 0) {
                Set<Map.Entry<Integer, Task>> entrySet = mapTasksReady.entrySet();
                for (Map.Entry<Integer, Task> entry : entrySet) {
                    entry.getValue().setTimeStamp(System.currentTimeMillis());
                    response = entry.getValue();
                    mapTasksInProgress.put(entry.getKey(), entry.getValue());
                    mapTasksReady.remove(entry.getKey());
                    System.out.printf("Distribute map task %d\n", response.getTaskID());
                    return response;
                }
            } else if (mapTasksInProgress.size() > 0) {
                response.setTaskType(Task.WAIT);
                return response;
            }

            //generate reduce task when all map tasks were done
            if (!reduceReady) {
                for (int i = 0; i < nReduce; i++) {
                    Task task = new Task();
                    task.setTaskType(Task.REDUCE);
                    task.setTaskID(i);
                    task.setnReduce(nReduce);
                    task.setnMap(nMap);
                    task.setTimeStamp(System.currentTimeMillis());
                    reduceTasksReady.put(i, task);
                }
                reduceReady = true;
            }

            if (reduceTasksReady.size() > 0) {
                Set<Map.Entry<Integer, Task>> entrySet = reduceTasksReady.entrySet();
                for (Map.Entry<Integer, Task> entry : entrySet) {
                    entry.getValue().setTimeStamp(System.currentTimeMillis());
                    response = entry.getValue();
                    reduceTasksInProgress.put(entry.getKey(), entry.getValue());
                    reduceTasksReady.remove(entry.getKey());
                    System.out.printf("Distribute reduce task %d\n", response.getTaskID());
                    return response;
                }
            } else if (reduceTasksInProgress.size() > 0) {
                response.setTaskType(Task.WAIT);
            } else {
                response.setTaskType(Task.DONE);
            }
            return response;
        }
    }

    @Override
    public void taskDone(Task request) throws RemoteException {
        synchronized (this) {
            switch (request.getTaskType()) {
                case Task.MAP -> {
                    mapTasksInProgress.remove(request.getTaskID());
                    System.out.printf("Map task %d done, %d tasks left\n", request.getTaskID(), mapTasksReady.size() + mapTasksInProgress.size());
                }
                case Task.REDUCE -> {
                    reduceTasksInProgress.remove(request.getTaskID());
                    System.out.printf("Reduce task %d done, %d tasks left\n", request.getTaskID(), reduceTasksReady.size() + reduceTasksInProgress.size());
                }
            }
        }
    }

    @Override
    public boolean done() {
        return mapTasksReady.size() == 0 && mapTasksInProgress.size() == 0 && reduceTasksReady.size() == 0 && reduceTasksInProgress.size() == 0;
    }
}
