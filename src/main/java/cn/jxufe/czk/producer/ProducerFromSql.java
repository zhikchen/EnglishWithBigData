package cn.jxufe.czk.producer;


import cn.jxufe.czk.util.DBUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class ProducerFromSql {
    private String topic;

    private Producer<Integer,String> producer;

    public ProducerFromSql(String topic){
        this.topic = topic;
        Properties properties = new Properties();

    }

    public static void main(String[] args) {
        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "master:9092");
        // 等待所有副本节点的应答
        props.put("acks", "1");
        // 消息发送最大尝试次数
        props.put("retries", 0);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        int i = 0;
        while(true){
            try{
                KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
                Connection connection = DBUtil.getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("select t.yuliao from yuliao t limit "+200*i+",200;");
                int count = 0;
                while(resultSet.next()){
                    count++;
                    producer.send(new ProducerRecord<String, String>("english", System.currentTimeMillis()+"", resultSet.getString(1)));
                }
                i++;
                producer.close();
                DBUtil.closeConnection(statement, connection);
                if(count<10) break;
            }catch (Exception e){

            }
        }


    }
}
