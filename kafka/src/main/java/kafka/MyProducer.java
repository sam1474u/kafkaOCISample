package kafka;

import java.io.FileInputStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.json.simple.JSONObject;

public class MyProducer {
    //public String myBotiques = "POS1,POS2,POS3,POS4,ARCHIVAL";
    public ArrayList<String> myBotiques = new ArrayList<String>(Arrays.asList("POS1","POS2","POS3","POS4","ARCHIVAL"));
    public static void main(String[] args) throws Exception{

      
        FileInputStream fis = new FileInputStream("C:\\sambha\\256kb-text.txt");
        String stringTooLong = IOUtils.toString(fis, "UTF-8");
      
        System.out.println("Message sent successfully");

    }
    
    public void publishCDMessage(List<JSONObject> listtopush){
        //String topicName = "Fanout-stream";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "cell-1.streaming.ap-mumbai-1.oci.oraclecloud.com:9092");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        //properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"sehubjapacprod/appdevuser/ocid1.streampool.oc1.eu-frankfurt-1.amaaaaaaak7gbriajwl6elul6i3xy75jzuwcdbz3nnukbmcrsw34yuae34uq\" password=\")7K>}EU}6sTnNn2]qYt)\";");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"titanpoc/oracleidentitycloudservice/Sambha/ocid1.streampool.oc1.ap-mumbai-1.amaaaaaahglgkiya4gjnjes5ylezcpz7ebrnvzc77wka453vics6glukpnnq\" password=\"+GoC#oG)kT-36GQi+5ao\";");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        for(JSONObject obj: listtopush){
            if(obj.get("MT").toString().equals("D")){
                    producer.send(new ProducerRecord<String, String>(obj.get("MD").toString(),obj.get("MK").toString(), obj.get("MP").toString()));
                    producer.send(new ProducerRecord<String, String>("ARCHIVAL",obj.get("MK").toString(), obj.toJSONString()));
            }else{
                for(String topicname: myBotiques){
                    if(topicname.equals("ARCHIVAL")){
                    producer.send(new ProducerRecord<String, String>(topicname,
                            obj.get("MK").toString(), obj.toJSONString()));
                    }else{
                        System.out.println(obj.get("MT"));
                        System.out.println(topicname);
                        System.out.println(obj.get("MD"));
    
                        
                                 producer.send(new ProducerRecord<String, String>(topicname,obj.get("MK").toString(), obj.get("MP").toString()));
    
                        
                    }
                    System.out.println(obj.get("MP").toString());
                }
            }
         
        }
        
        producer.close();

        
    }
    
    public void publishPriorityMessage(List<JSONObject> listtopush){
        //String topicName = "Fanout-stream";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "cell-1.streaming.ap-mumbai-1.oci.oraclecloud.com:9092");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        //properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"sehubjapacprod/appdevuser/ocid1.streampool.oc1.eu-frankfurt-1.amaaaaaaak7gbriajwl6elul6i3xy75jzuwcdbz3nnukbmcrsw34yuae34uq\" password=\")7K>}EU}6sTnNn2]qYt)\";");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"titanpoc/oracleidentitycloudservice/Sambha/ocid1.streampool.oc1.ap-mumbai-1.amaaaaaahglgkiya4gjnjes5ylezcpz7ebrnvzc77wka453vics6glukpnnq\" password=\"+GoC#oG)kT-36GQi+5ao\";");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        for(JSONObject obj: listtopush){
             String topicname = obj.get("MD").toString()+".P";
             producer.send(new ProducerRecord<String, String>(topicname,obj.get("MK").toString(), obj.get("MP").toString()));
             String Arctopicname = "ARCHIVAL";
             producer.send(new ProducerRecord<String, String>(Arctopicname,obj.get("MK").toString(), obj.toJSONString()));   
           
            
         
        }
        
        producer.close();
    
    }
    
    public void produce(List<JSONObject> CDList, List<JSONObject> PList){
        if(CDList.size() > 0){
            this.publishCDMessage(CDList);
        }
        if(PList.size() > 0){
            this.publishPriorityMessage(PList);
        }
        
    }
}