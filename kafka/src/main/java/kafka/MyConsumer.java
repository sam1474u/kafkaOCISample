package kafka;

import java.io.FileInputStream;

import java.io.FileNotFoundException;

import java.io.IOException;

import java.sql.DatabaseMetaData;

import java.sql.SQLException;

import java.util.Properties;
import java.util.Arrays;

import java.util.List;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class MyConsumer {
    final  String DB_URL= "jdbc:oracle:thin:@140.238.166.235:1521/DB1112_bom1c6.sm15lb.sm15vcn.oraclevcn.com";
    // For ATP and ADW - use the TNS Alias name along with the TNS_ADMIN when using 18.3 JDBC driver
    // final static String DB_URL="jdbc:oracle:thin:@wallet_dbname?TNS_ADMIN=/Users/test/wallet_dbname";
    // In case of windows, use the following URL 
    // final static String DB_URL="jdbc:oracle:thin:@wallet_dbname?TNS_ADMIN=C:/Users/test/wallet_dbname";
    final  String DB_USER = "sys as sysdba";
    final  String DB_PASSWORD = "WElcome#1234-";
    public static void main(String[] args) throws Exception {

        String topic = "POS1";

        Properties props = new Properties();
       // props.put("bootstrap.servers", "cell-1.streaming.us-ashburn-1.oci.oraclecloud.com:9092");
       props.put("bootstrap.servers", "cell-1.streaming.ap-mumbai-1.oci.oraclecloud.com:9092");

        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        //props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"titanstream/oracleidentitycloudservice/vishal.vb.bhardwaj@oracle.com/ocid1.streampool.oc1.iad.amaaaaaa5rgbmwiam2pphns476s36lserohvzukrmdyt36ycqvosf3j3vxsa\" password=\"m38mFHSoKyNil7..9w(2\";");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"titanpoc/oracleidentitycloudservice/Sambha/ocid1.streampool.oc1.ap-mumbai-1.amaaaaaahglgkiya4gjnjes5ylezcpz7ebrnvzc77wka453vics6glukpnnq\" password=\"+GoC#oG)kT-36GQi+5ao\";");
        //props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"sehubjapacprod/appdevuser/ocid1.streampool.oc1.eu-frankfurt-1.amaaaaaaak7gbriajwl6elul6i3xy75jzuwcdbz3nnukbmcrsw34yuae34uq\" password=\")7K>}EU}6sTnNn2]qYt)\";");

//        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{tenancyName}/{username}/{stream pool OCID}\" password=\"{authToken}\";");
        props.put("group.id", "mygrp");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
   //     props.put("max.poll.records", "1");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList(topic));
        System.out.println("Subscribed to topic " + topic);
//        int i = 0;
//
//            ConsumerRecords<String, String> records = consumer.poll(100);
//            for (ConsumerRecord<String, String> record : records){
//                System.out.printf("offset = %d, key = %s, value = %s\n",
//                        record.offset(), record.key(), record.value());
//            }
                long startTime = System.currentTimeMillis();

         int noRecordsCount = 0; 
         final int giveUp = 100;
         String CurrentKey = null;
         int count = 0;
        ConsumerRecords<String, String> records  = null;
         while(true){
                records = consumer.poll(1000);
                if(!records.isEmpty()){
                 break;
                }
         }
        for (ConsumerRecord<String, String> record : records)
        { 
            CurrentKey = record.key();
        }
        
        String[] cuK = CurrentKey.split("_");
        FileInputStream fis = new FileInputStream("C:\\sambha\\CloudNative\\Titan\\LastReadRecord.txt");
        String lastRead = IOUtils.toString(fis, "UTF-8");
        boolean readContinue = true;
        if(lastRead != null){
            String[] lastReadcom = lastRead.split("_");
                int CukDate = Integer.parseInt(cuK[0]);
                int lastDate = Integer.parseInt(lastReadcom[0]);
                int Cukpos = Integer.parseInt(cuK[1]);
                System.out.println(lastReadcom[1]);
                int lastpos = Integer.parseInt(lastReadcom[1]);
                if(! (CukDate == lastDate)){
                    readContinue = false;
                } 
                if(!(Cukpos == lastpos+1) ){
                    readContinue = false;
                } 
                if(readContinue){
                    readContinue(records);
                }else{
                    ReadFromArchive rd = new ReadFromArchive();
                    List<String> missedRecords =  rd.readArchive("POS", lastRead, CurrentKey);
                   
                
                }
                
            
        }
        consumer.close();
        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;
       // System.out.println("#########################"+timeElapsed);
        System.out.println("DONE");
        
    }
    public static void readContinue(ConsumerRecords<String,String> records){
        System.out.println("readContinue");
                            for (ConsumerRecord<String, String> record : records)
                            {  
                                 System.out.println(record.key());
                            }
                    
    }
       

}
