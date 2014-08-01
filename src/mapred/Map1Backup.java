/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred;

import app.ItemTid;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author eduardo
 */
public class Map1Backup extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
    
    IntWritable chave = new IntWritable();
    static IntWritable count = new IntWritable(1);
    Log log = LogFactory.getLog(Map1Backup.class);
    static int transactions = 0;
    
    MultipleOutputs<String, Text> mo;
    
    
    ItemTid itemTid = new ItemTid();
    
    
    @Override
    public void setup(Context c){
        
        log.info("Iniciando o primeiro MAP");
        
        mo = new MultipleOutputs(c);
        
        /*Criar arquivo invertido*/
    }
    
    @Override
    public void map(LongWritable tid, Text value, Context context) throws IOException{
        
        transactions++;
        
        StringTokenizer token = new StringTokenizer(value.toString());
        String t;
        LongWritable tids = new LongWritable(transactions);
//        log.info("Transacao: "+tids);
        
        while(token.hasMoreTokens()){
            t = token.nextToken();
            
//            log.info("Inserindo na HASH... "+t+" "+tids);
            itemTid.putValue(t, tids);
            
            try {
                context.write(new IntWritable(Integer.parseInt(t)), count);
            } catch (InterruptedException ex) {
                Logger.getLogger(Map1Backup.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    @Override
    public void cleanup(Context c){
        
        String out = "/user/hadoop/invert/1item";
        
        checkAndDelExist(c, out);
        
        log.info("Salva o arquivo invertido em txt...");
        
        Set<String> keys = itemTid.keySet();
        
        ArrayList<LongWritable> v;
        StringBuilder sb;
        
        for(String k: keys){
            sb = new StringBuilder();
            v = itemTid.get(k);
            
            for(LongWritable t: v){
                sb.append(t).append("-");
            }
            sb.append("\n");
            
//            log.info("Key: "+k+" values: "+sb.toString());
            try {
                mo.write(k, new Text(sb.toString()), out);
                
            } catch (    IOException | InterruptedException ex) {
                Logger.getLogger(Map1Backup.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        }
        try {
            mo.close();
        } catch (IOException | InterruptedException ex) {
            Logger.getLogger(Map1Backup.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    public void checkAndDelExist(Context c, String way){
        Path p1 = new Path(way+"-r-00000");
        Path p2 = new Path(way+"-m-00000");
        
        delFiles(c, p1);
        delFiles(c, p2);
        
    }
    
    public void delFiles(Context c, Path p){
        try {
            FileSystem fs = p.getFileSystem(c.getConfiguration());
            
            if(fs.exists(p)){
                fs.delete(p, true);
            }
            
        } catch (IOException ex) {
            Logger.getLogger(Map1Backup.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
}
