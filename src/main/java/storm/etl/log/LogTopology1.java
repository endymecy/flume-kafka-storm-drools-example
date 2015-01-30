package storm.etl.log;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.apache.zookeeper.client.ConnectStringParser;
import org.slf4j.Logger;
import storm.etl.log.bolt.LogRulesBolt;
import storm.etl.log.bolt.SaveToRedisCloudBolt;
import storm.kafka.*;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LogTopology1 {
    public static  Logger logger= org.slf4j.LoggerFactory.getLogger(LogTopology1.class);
    public LogTopology1() {

    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        if (args != null && args.length > 0) {
            try{
                BrokerHosts brokerHosts = new ZkHosts("10.132.174.98");
                SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "fks", "", "storm");
                spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
                spoutConfig.startOffsetTime=-2;
                spoutConfig.forceFromStart=true;
                KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
                TopologyBuilder topologyBuilder = new TopologyBuilder();

                topologyBuilder.setSpout("logSpout", kafkaSpout,2);
                topologyBuilder.setBolt("logRules", new LogRulesBolt(args[0]),10).shuffleGrouping("logSpout");
                topologyBuilder.setBolt("saveToDB", new SaveToRedisCloudBolt(),10).shuffleGrouping("logRules");

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
            BrokerHosts brokerHosts = new ZkHosts("10.132.174.98");
            SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "fks", "", "storm");
            spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            spoutConfig.startOffsetTime=-2;
            spoutConfig.forceFromStart=true;
            KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
            TopologyBuilder topologyBuilder = new TopologyBuilder();

            topologyBuilder.setSpout("logSpout", kafkaSpout,2);
            topologyBuilder.setBolt("logRules", new LogRulesBolt("D:\\etl log analysis\\logtopology\\target\\classes\\Syslog.drl"),10).shuffleGrouping("logSpout");
            topologyBuilder.setBolt("saveToDB", new SaveToRedisCloudBolt(),10).shuffleGrouping("logRules");

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, topologyBuilder.createTopology());
            Utils.sleep(5000000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

}
