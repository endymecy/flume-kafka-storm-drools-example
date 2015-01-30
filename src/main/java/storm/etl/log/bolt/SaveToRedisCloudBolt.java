package storm.etl.log.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import storm.etl.log.bean.LogEntry;
import storm.etl.log.bean.MessageToRedis;

import java.util.List;
import java.util.Map;

public class SaveToRedisCloudBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

    private static final Logger logger= org.slf4j.LoggerFactory.getLogger(SaveToRedisCloudBolt.class);
    private static final String LOG_ENTRY = "str";
    private int expireTime=3600*24*7;//過期時間 7天
    private OutputCollector collector;

    private Jedis jedis;

    public  void   insertToRedis(List<MessageToRedis> redisRecords) throws Exception{
        for(MessageToRedis message :redisRecords)
        {
            String key=message.getKey();
            try{
                switch (message.getType())
                {
                    case LONG:
                        jedis.incrBy(key,Long.parseLong(message.getValue()));
                        break;
                    case STRING:
                        jedis.set(key,message.getValue());
                        break;
                    case SET:
                        jedis.zadd(key,Double.parseDouble(message.getValue()),message.getValue());
                        break;
                    default:break;
                }
                jedis.expire(key,expireTime);
            }catch (Exception e)
            {
               throw new Exception(e.getMessage()+" (error message is："+message+")");
            }
        }
    }

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        this.jedis=new Jedis("10.132.174.98", 6379);
    }

    @Override
    public void execute(Tuple input) {
        LogEntry entry=(LogEntry) input.getValueByField(LOG_ENTRY);
        List<MessageToRedis> redisRecords=entry.getMessagesToRedis();
        try {
            if(redisRecords.isEmpty())
                logger.warn("pair is null,so this is not data to save");
            else
                insertToRedis(redisRecords);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        finally {
            collector.ack(input);
        }
    }
}

