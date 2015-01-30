package storm.etl.log;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import storm.etl.log.bolt.LogRulesBolt;
import storm.etl.log.bolt.SaveToRedisBolt;
import storm.etl.log.bolt.SaveToRedisCloudBolt;
import storm.kafka.*;

import java.util.Arrays;

public class LogTopologyLocal {
    public static  Logger logger= org.slf4j.LoggerFactory.getLogger(LogTopologyLocal.class);
    public LogTopologyLocal() {

    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        if (args != null && args.length > 0) {
            try{
                ZkHosts hosts=new ZkHosts("10.200.187.71");
                SpoutConfig spoutConfig=new SpoutConfig(hosts,"fks2", "", "storm");
                spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
                spoutConfig.startOffsetTime=-2l;
                spoutConfig.forceFromStart=true;
                spoutConfig.zkServers = Arrays.asList("10.200.187.71","10.200.187.73","10.200.187.57");
                spoutConfig.zkPort = 2181;
                KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
                TopologyBuilder topologyBuilder = new TopologyBuilder();

                topologyBuilder.setSpout("logSpout", kafkaSpout, 1);
                topologyBuilder.setBolt("logRules", new LogRulesBolt(args[0]),5).shuffleGrouping("logSpout");
                topologyBuilder.setBolt("saveToDB", new SaveToRedisBolt(),5).shuffleGrouping("logRules");
                conf.setNumWorkers(3);

                conf.put(Config.NIMBUS_HOST, "10.200.187.71");
                conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
                conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("10.200.187.71","10.200.187.73","10.200.187.57"));

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
            ZkHosts hosts=new ZkHosts("10.200.187.71");
            SpoutConfig spoutConfig=new SpoutConfig(hosts,"fks2", "", "storm");
            spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            spoutConfig.startOffsetTime=-2l;
            spoutConfig.forceFromStart=true;
            spoutConfig.zkServers = Arrays.asList("10.200.187.71","10.200.187.73","10.200.187.57");
            spoutConfig.zkPort = 2181;
            KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
            TopologyBuilder topologyBuilder = new TopologyBuilder();

            topologyBuilder.setSpout("logSpout", kafkaSpout, 1);
            topologyBuilder.setBolt("logRules", new LogRulesBolt(args[0]),5).shuffleGrouping("logSpout");
            topologyBuilder.setBolt("saveToDB", new SaveToRedisBolt(),5).shuffleGrouping("logRules");

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, topologyBuilder.createTopology());
            Utils.sleep(5000000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

}
