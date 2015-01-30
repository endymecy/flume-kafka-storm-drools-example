package storm.etl.log;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import nl.minvenj.nfi.storm.kafka.KafkaSpout;
import org.slf4j.Logger;
import storm.etl.log.bolt.LogRulesBolt;
import storm.etl.log.bolt.SaveToRedisCloudBolt;

import java.util.Arrays;

public class LogTopology {
    public static  Logger logger= org.slf4j.LoggerFactory.getLogger(LogTopology.class);
    public LogTopology() {

    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        if (args != null && args.length > 0) {
            try{
                conf.put("kafka.spout.topic", "fks");
                conf.put("kafka.group.id", "storm_group");
                conf.put("kafka.zookeeper.connect", "10.132.174.98:2181");
                conf.put("kafka.consumer.timeout.ms", 100000);
                conf.put("kafka.zookeeper.session.timeout.ms", "40000");
                conf.put("kafka.spout.buffer.size.max", "512");

                TopologyBuilder topologyBuilder = new TopologyBuilder();
                topologyBuilder.setSpout("logSpout", new KafkaSpout(),1);
                topologyBuilder.setBolt("logRules", new LogRulesBolt(args[0]),15).shuffleGrouping("logSpout");
                topologyBuilder.setBolt("saveToDB", new SaveToRedisCloudBolt(),15).shuffleGrouping("logRules");

                conf.setNumWorkers(5);


                conf.put(Config.NIMBUS_HOST, "10.132.176.117");
                conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
                conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("10.132.176.117","10.132.175.107","10.132.163.27"));

                conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
                conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
                conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
                conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
                StormSubmitter.submitTopology(args[1], conf, topologyBuilder.createTopology());
            }
            catch(Exception e)
            {
                logger.error("this topology run fail!");
            }
        }
        else
        {
            conf.put("kafka.spout.topic", "fks");
            conf.put("kafka.group.id", "storm_group");
            conf.put("kafka.zookeeper.connect", "10.132.174.98:2181");
            conf.put("kafka.consumer.timeout.ms", 100000);
            conf.put("kafka.zookeeper.session.timeout.ms", "40000");
            TopologyBuilder topologyBuilder = new TopologyBuilder();
            topologyBuilder.setSpout("logSpout", new KafkaSpout(),1);
            topologyBuilder.setBolt("logRules", new LogRulesBolt("D:\\etl log analysis\\logtopology\\target\\classes\\Syslog.drl"),5).shuffleGrouping("logSpout");
            topologyBuilder.setBolt("saveToDB", new SaveToRedisCloudBolt(),5).shuffleGrouping("logRules");

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, topologyBuilder.createTopology());
            Utils.sleep(500000);
            cluster.killTopology("test");
            cluster.shutdown();

        }
    }

}
