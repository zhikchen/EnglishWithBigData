package cn.jxufe.czk.service;

import cn.jxufe.czk.util.HBaseUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class TwoTuplesService {

    public static void saveToHbase(ConsumerRecord<String, String> record) throws Exception{
        Connection con = HBaseUtil.getConnection();
        HBaseUtil.putData(con,"two-tuples", record.key(),"info","num",record.value());
        HBaseUtil.closeConnection(con);
    }
}
