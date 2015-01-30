package storm.etl.log.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LogSpout extends BaseRichSpout {

    public static final String LOG_ENTRY = "str";
    private  int curIdx=0;
    private SpoutOutputCollector collector;
    private List<String> logEntry1s;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(LOG_ENTRY));
    }

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.logEntry1s=getJsonObj();
    }

    public List<String> getJsonObj()
    {
        List<String> list=new ArrayList<String>();
        String str1="tansfomer-success$PlatformId1$ShopTagId1$3.25$"+System.currentTimeMillis();
        String str2="tansfomer-error$PlatformId2$ShopTagId2$"+System.currentTimeMillis();

        String str3="loader-success$entity1$update$mysql$PlatformId3$ShopTagId3$4.43$"+System.currentTimeMillis();
        String str4="loader-error$entity2$modify$mysql$PlatformId4$ShopTagId4$"+System.currentTimeMillis();
        String str5="loader-success$entity3$update$mysql$PlatformId3$ShopTagId3$8.43$"+System.currentTimeMillis();
        String str6="loader-error$entity4$modify$mysql$PlatformId4$ShopTagId4$"+System.currentTimeMillis();
        String str7="loader-success$entity5$update$mysql$PlatformId3$ShopTagId3$11$"+System.currentTimeMillis();
        String str8="loader-error$entity6$modify$mysql$PlatformId4$ShopTagId4$"+System.currentTimeMillis();
        list.add(str1);
        list.add(str2);
        list.add(str3);
        list.add(str4);
        list.add(str5);
        list.add(str6);
        list.add(str7);
        list.add(str8);
        return  list;
    }

    @Override
    public void nextTuple() {
        if(curIdx<logEntry1s.size())
        {
            String entry=logEntry1s.get(curIdx).trim();
             collector.emit(new Values(entry));
             curIdx++;
         }
    }

    public static void main(String [] args)
    {
//        String str1="tansfomer-success$PlatformId1$ShopTagId1$200$"+System.currentTimeMillis();
//        String str2="tansfomer-error$PlatformId2$ShopTagId2$"+System.currentTimeMillis();
//        String str3="loader-success$entity1$update$mysql$PlatformId3$ShopTagId3$400$"+System.currentTimeMillis();
//        String str4="loader-error$entity2$modify$mysql$PlatformId4$ShopTagId4$"+System.currentTimeMillis();
//        System.out.println(str1);
//        System.out.println(str2);
//        System.out.println(str3);
//        System.out.println(str4);
        System.out.println(Float.parseFloat(".12"));
    }
}
