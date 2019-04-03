package cn.jxufe.czk.service;

import cn.jxufe.czk.util.HBaseUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class YuliaoService {

    public static void saveToHbase(ConsumerRecord<String, String> record) throws Exception{
        Connection con = HBaseUtil.getConnection();
        HBaseUtil.putData(con,"English", record.key(),"info","content",record.value());
        String[] lines = record.value().split("\\.");
        for(int i=0;i<lines.length;i++){
            String[] words = lines[i].split("[\"|,|\" \"|(|)|?|:|']");
            for(int j =0;j<words.length;j++){
                if(!words[j].equals("")){
                    HBaseUtil.putData(con,"word",words[j].toLowerCase(),"info",record.key()+"+"+i,String.valueOf(j));
                }
            }
        }
        HBaseUtil.closeConnection(con);
    }
}
