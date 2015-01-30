package storm.etl.log.bean;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogEntry implements Serializable{
    private String logContent;
    private List<MessageToRedis> messagesToRedis=new ArrayList<MessageToRedis>();


    public List<MessageToRedis> getMessagesToRedis() {
        return messagesToRedis;
    }

    public LogEntry(String logContent)
    {
        this.logContent=logContent;

    }

    public String getLogContent() {
        return logContent;
    }

    public String  getType()
    {
        return logContent.split("\\$")[0];
    }

}
