package storm.etl.log.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Endy on 14-7-17.
 */
public class JedisUtil {
    private static  Logger log = LoggerFactory.getLogger(JedisUtil.class);
    private static JedisPool pool = null;
    public static JedisPool getPool(String ip,int port) {
        JedisPoolConfig config = new JedisPoolConfig();
         config.setMaxTotal(100);
         config.setMaxIdle(30);
        if(pool==null)
        {
            try{
                pool = new JedisPool(config, ip, port,5000);
            } catch(Exception e) {
                log.debug("Get jedis pool error !");
                e.printStackTrace();
            }
        }
        return pool;
    }
}
