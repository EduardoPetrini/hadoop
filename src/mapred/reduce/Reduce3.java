/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred.reduce;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author eduardo
 */
public class Reduce3 extends Reducer<Text, Text, Text, Text> {
    
    Log log = LogFactory.getLog(Reduce3.class);
    SequenceFile.Writer writer;
    MultipleOutputs<Text, Text> mo;
    String count;
    
    @Override
    public void setup(Context c) throws IOException{
        count = c.getConfiguration().get("count");
        log.info("Iniciando o REDUCE 3. Count dir: "+count);
        
        writer = SequenceFile.createWriter(c.getConfiguration(), SequenceFile.Writer.file(new Path("/user/hadoop/invert/invertido"+count)),
               SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(Text.class));
        mo = new MultipleOutputs(c);
    }
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context){
        
        Text v = values.iterator().next();
        try {
            v.set(v.toString().replace(" ", ""));
            if((key.toString().split("-").length % 2) == 0){
                context.write(key, v);
                save(key, v);
            }else{
                outputInter(key, v);
                
            }
        } catch (IOException | InterruptedException ex) {
            Logger.getLogger(Reduce3.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     * 
     * @param key
     * @param value 
     */
    public void save(Text key, Text value){
        try {
            writer.append(key, value);
        } catch (IOException ex) {
            Logger.getLogger(Reduce3.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void outputInter(Text key, Text value){
        String out = "/user/hadoop/output"+count+"1/out";
        
        
        try {
            mo.write(key, value, out);
        } catch (IOException | InterruptedException ex) {
            Logger.getLogger(Reduce3.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    @Override
    public void cleanup(Context c){
        log.info("Finalizando o REDUCE 3.");
        try {
            writer.close();
            mo.close();
        } catch (IOException | InterruptedException ex) {
            Logger.getLogger(Reduce3.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
