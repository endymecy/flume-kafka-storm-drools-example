package storm.etl.log.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseFactory;
import org.drools.builder.KnowledgeBuilder;
import org.drools.builder.KnowledgeBuilderFactory;
import org.drools.builder.ResourceType;
import org.drools.io.ResourceFactory;
import org.drools.runtime.StatelessKnowledgeSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.etl.log.bean.LogEntry;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;

/**
 * Created by endy on 14-9-25.
 */
public class LogRulesBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    Logger logger= LoggerFactory.getLogger(LogRulesBolt.class);
    public static final String LOG_ENTRY = "str";
    private StatelessKnowledgeSession ksession;
    private String drlFile;
    private OutputCollector collector;

    public LogRulesBolt(String drlFile)
    {
        this.drlFile=drlFile;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        KnowledgeBuilder kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();
        try {
            kbuilder.add( ResourceFactory.newInputStreamResource(new FileInputStream(new File(drlFile))), ResourceType.DRL );
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage());
        }
        KnowledgeBase kbase = KnowledgeBaseFactory.newKnowledgeBase();
        kbase.addKnowledgePackages( kbuilder.getKnowledgePackages() );
        ksession = kbase.newStatelessKnowledgeSession();
    }

    @Override
    public void execute(Tuple input) {
        String logContent=(String)input.getValueByField(LOG_ENTRY);
        logContent=logContent.trim();
        if(!"".equals(logContent)&&logContent!=null)
        {
            LogEntry entry = new LogEntry(logContent);
            try{
                ksession.execute( entry );
            }catch(Exception e)
            {
                logger.error("drools to handle log ["+logContent+"] is failure !");
                logger.error(e.getMessage());
            }

            collector.emit(new Values(entry));
        }
        else
        {
            logger.error("log content is empty !");
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(LOG_ENTRY));
    }
}
