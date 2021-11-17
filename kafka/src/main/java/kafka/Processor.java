package kafka;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Date;

import java.util.List;

import kafka.utils.json.JsonObject;

import org.json.simple.JSONObject;


public class Processor {
    volatile static int n = 6020;  
    volatile static int p = 1000;  

    public Processor() {
        super();
    }
    
    /*
     * Metadata - M
     * Type   - C,P,D
     * Destination - D
     * P - Payload
     * K - Date+Sequence (Starts from 1)
     */
    List<JSONObject> cdlist = new ArrayList<JSONObject>();
    List<JSONObject> pList = new ArrayList<JSONObject>();
    public void createMetaData(String type,String destination,String payload){
            SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");  
            Date date = new Date();  
            String Sdate = (formatter.format(date)).replace("/", "");
      
       

            if(type.equals("C") || type.equals("D") ){
                int sequence = nextNum();
                String DataandSeq = Sdate+"_"+sequence;
                System.out.println(DataandSeq);
                JSONObject mobj = new JSONObject();
                mobj.put("MT",type);
                mobj.put("MD",destination);
                mobj.put("MP",payload);
                mobj.put("MK",DataandSeq);
                cdlist.add(mobj);
            }
            if(type.equals("P")){
                int sequence = nexPtNum();
                String DataandSeq = Sdate+"_"+sequence+"_"+"P";
                System.out.println(DataandSeq);
                JSONObject mobj = new JSONObject();
                mobj.put("MT",type);
                mobj.put("MD",destination);
                mobj.put("MP",payload);
                mobj.put("MK",DataandSeq);
                pList.add(mobj);
            }
            
            //System.out.println(mobj.toString());
    }
    
    public synchronized int nextNum(){
           return n++;
    }  
    
    public synchronized int nexPtNum(){
           return p++;
    }  
    
    public void produceMsg(){
        MyProducer mp = new MyProducer();
        mp.produce(cdlist, pList);
    }
    
    public static void main(String args[]){
        Processor p = new Processor();
     
        p.createMetaData("C", "POS1", "C1");
        p.createMetaData("C", "POS1", "C2");

        
        p.createMetaData("D", "POS4", "testDirect");

        p.createMetaData("P", "POS1", "P1Message");
        p.createMetaData("P", "POS2", "P2Message");


        p.produceMsg();
       
    }
    

}
