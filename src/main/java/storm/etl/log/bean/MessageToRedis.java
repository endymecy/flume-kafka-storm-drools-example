package storm.etl.log.bean;

import storm.etl.log.util.DataType;

import java.io.Serializable;

/**
 * Created by endy on 14-10-16.
 */
public class MessageToRedis  implements Serializable{
    private String key;
    private String value;
    private DataType type;


    public MessageToRedis(String key,String value,DataType type)
    {
        this.key=key;
        this.value=value;
        this.type=type;
    }
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public DataType getType() {
        return type;
    }

    public void setType(DataType type) {
        this.type = type;
    }
}
