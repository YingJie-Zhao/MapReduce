import java.io.Serializable;

public class Task implements Serializable {

    public static final int MAP = 0;
    public static final int REDUCE = 1;
    public static final int WAIT = 2;
    public static final int DONE = 3;

    private String fileName;
    private int taskType;
    private int taskID;
    private int nReduce;
    private int nMap;
    private long timeStamp;

    public Task() {
    }

    public Task(String fileName, int taskType, int taskID, int nReduce, int nMap, long timeStamp) {
        this.fileName = fileName;
        this.taskType = taskType;
        this.taskID = taskID;
        this.nReduce = nReduce;
        this.nMap = nMap;
        this.timeStamp = timeStamp;
    }

    public int getnReduce() {
        return nReduce;
    }

    public void setnReduce(int nReduce) {
        this.nReduce = nReduce;
    }


    public int getnMap() {
        return nMap;
    }

    public void setnMap(int nMap) {
        this.nMap = nMap;
    }

    public static int getMAP() {
        return MAP;
    }

    public static int getREDUCE() {
        return REDUCE;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getTaskType() {
        return taskType;
    }

    public void setTaskType(int taskType) {
        this.taskType = taskType;
    }

    public int getTaskID() {
        return taskID;
    }

    public void setTaskID(int taskID) {
        this.taskID = taskID;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }
}
