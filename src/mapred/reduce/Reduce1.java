/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred.reduce;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author eduardo
 */
public class Reduce1 extends Reducer<IntWritable, Text, IntWritable, Text>{
    
    Log log = LogFactory.getLog(Reduce1.class);
    SequenceFile.Writer writer;
    double support;
    Text out = new Text();
    
    @Override
    public void setup(Context c) throws IOException{
        String count = c.getConfiguration().get("count");
        support = Integer.parseInt(c.getConfiguration().get("support"));
        
        log.info("Iniciando o REDUCE 1. Count Dir: "+count);
        log.info("Reduce1 support = "+support);
        
        writer = SequenceFile.createWriter(c.getConfiguration(), SequenceFile.Writer.file(new Path("/user/eduardo/invert/invertido"+count)),
               SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Text.class));
        
        
    }
    
    public ArrayList<Integer> sortValue(Iterable<Text> values){
        
        ArrayList<Integer> aux = new ArrayList();
        
        for (Iterator<Text> it = values.iterator(); it.hasNext();) {
            
            aux.add(Integer.parseInt(it.next().toString()));
        }

        Collections.sort(aux);
        
        return aux;
        
    }
    
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context){
        
        ArrayList<Integer> aux = sortValue(values);
        int count = aux.size();
        
        if(count > support){
        
            try {
                
                out.set(aux.toString().replace(" ", "")); 
                
                save(key, out);
                context.write(key, out);

            } catch (IOException | InterruptedException ex) {
                Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
//    @Override
//    public void reduce(IntWritable key, Iterable<Text> values, Context context){
//        
//        ArrayList<Integer> v = sortValue(values);
//        StringBuilder sb = new StringBuilder();
//        
//        for(Integer i: v){
//            sb.append(i).append(":");
//        }
//        
//        try {
//            if(v.size() >= support){
//                
//                out.set(sb.toString());
//                context.write(key, out);
//                save(key, out);
//            }
//            
//        } catch (IOException | InterruptedException ex) {
//            Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
//        }
//    }
//    
    public void save(IntWritable key, Text value){
        try {
            writer.append(key, value);
        } catch (IOException ex) {
            Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    public void cleanup(Context c){
        log.info("Finalizando o REDUCE 1.");
        
        try {
            writer.close();
        } catch (IOException ex) {
            Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
}
