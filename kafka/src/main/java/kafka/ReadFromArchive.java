package kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.json.simple.JSONObject;

public class ReadFromArchive {
    public ReadFromArchive() {
        super();
    }
    List<String> missedList = new ArrayList<String>();

    public List<String> readArchive(String forPOS,String lastRecordKey , String currentRecordKey){
        String topic = "ARCHIVAL";

        Properties props = new Properties();
        // props.put("bootstrap.servers", "cell-1.streaming.us-ashburn-1.oci.oraclecloud.com:9092");
        props.put("bootstrap.servers", "cell-1.streaming.ap-mumbai-1.oci.oraclecloud.com:9092");

        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        //props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"titanstream/oracleidentitycloudservice/vishal.vb.bhardwaj@oracle.com/ocid1.streampool.oc1.iad.amaaaaaa5rgbmwiam2pphns476s36lserohvzukrmdyt36ycqvosf3j3vxsa\" password=\"m38mFHSoKyNil7..9w(2\";");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"titanpoc/oracleidentitycloudservice/Sambha/ocid1.streampool.oc1.ap-mumbai-1.amaaaaaahglgkiya4gjnjes5ylezcpz7ebrnvzc77wka453vics6glukpnnq\" password=\"+GoC#oG)kT-36GQi+5ao\";");
        //props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"sehubjapacprod/appdevuser/ocid1.streampool.oc1.eu-frankfurt-1.amaaaaaaak7gbriajwl6elul6i3xy75jzuwcdbz3nnukbmcrsw34yuae34uq\" password=\")7K>}EU}6sTnNn2]qYt)\";");

        //        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{tenancyName}/{username}/{stream pool OCID}\" password=\"{authToken}\";");
        props.put("group.id", forPOS);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        int noRecordsCount = 0; 
        final int giveUp = 1000;
        int count = 0;
        boolean StartStop = false;
        boolean exitwhile = false;
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
               ConsumerRecords<String, String> records = consumer.poll(1000);
               for (ConsumerRecord<String, String> record : records)
               {   
                 // System.out.println(record.key());
                 // System.out.println(lastRecordKey);
                 // System.out.println(currentRecordKey);
                 // System.out.println(StartStop);
                  // System.out.printf(",");
                
                   if(lastRecordKey.contains(record.key().toString())){
                      // MissedList.add(record.value());
                      StartStop = true;
                   }
                   if(currentRecordKey.contains(record.key().toString())){
                      // MissedList.add(record.value());
                      StartStop = false;
                       exitwhile = true;
                       break;
                   }
                   if(StartStop){
                        missedList.add(record.value());
                        if(!record.key().contains("_P")){
                                                System.out.println(record.value());}
                   }
                  
                   //System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                   //System.out.printf("###################################################################################################################################");

                       count= count+1;
               }
               if(count >= giveUp){
                   break;
               }
            if(exitwhile){
                break;
            }
             

        }
        consumer.close();
        //System.out.println("DONE");

        return missedList;
    }
    
    public static void main(String args[]){
        ReadFromArchive rd = new ReadFromArchive();
        List<String> missedRecords =  rd.readArchive("POS1", "12112021_300", "12112021_302");
        for(String missedRecord : missedRecords){
            System.out.println(missedRecord);
        }
    }
}
