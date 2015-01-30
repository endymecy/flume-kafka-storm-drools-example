package storm.etl.log.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import storm.etl.log.bean.LogEntry;
import storm.etl.log.bean.MessageToRedis;

import java.util.List;
import java.util.Map;

public class SaveToRedisBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
    public static final Logger logger= org.slf4j.LoggerFactory.getLogger(SaveToRedisBolt.class);
    public static final String LOG_ENTRY = "str";
    private OutputCollector collector;

    private Jedis jedis;
    private  Pipeline pipeline;

    public  void   insertToRedis(List<MessageToRedis> redisRecords) {
        for(MessageToRedis message :redisRecords)
        {
            switch (message.getType())
            {
                case LONG:
                    pipeline.incrBy(message.getKey(),Long.parseLong(message.getValue()));
                    break;
                case STRING:
                    pipeline.set(message.getKey(),message.getValue());
                    break;
                case SET:
                    pipeline.zadd(message.getKey(),Double.parseDouble(message.getValue()),message.getValue());
                    break;
                default:break;
            }
        }
        pipeline.sync();
    }

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        this.jedis=new Jedis("10.200.187.73", 6379);
        this.pipeline=this.jedis.pipelined();
    }

    @Override
    public void execute(Tuple input) {
        LogEntry entry=(LogEntry) input.getValueByField(LOG_ENTRY);
        List<MessageToRedis> redisRecords=entry.getMessagesToRedis();
        try{
            if(redisRecords.isEmpty())
            {
                logger.error("pair is null,so this is not data to save");
            }
            else
            {
                insertToRedis(redisRecords);
            }
        }catch (Exception e)
        {
            logger.error(e.getMessage());
        }
        finally {
            collector.ack(input);
        }
    }
}

