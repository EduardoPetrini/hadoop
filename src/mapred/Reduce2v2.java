/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Salva o conjunto de 2itemsets e seus respectivos tids.
 * @author eduardo
 */
public class Reduce2v2 extends Reducer<Text, Text, Text, Text> {
    
    Log log = LogFactory.getLog(Reduce2v2.class);
    SequenceFile.Writer writer;
    
    @Override
    public void setup(Context c) throws IOException{
        String count = c.getConfiguration().get("count");
        
        log.info("Iniciando o REDUCE 2. Count dir: "+count);
        
         writer = SequenceFile.createWriter(c.getConfiguration(), SequenceFile.Writer.file(new Path("/user/hadoop/invert/invertido"+count)),
               SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(Text.class));
    }
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context){
        Text v = values.iterator().next();
        
        try {
            v.set(v.toString().replace(" ", ""));
            context.write(key, v);
            save(key, v);
        } catch (IOException | InterruptedException ex) {
            Logger.getLogger(Reduce2v2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void save(Text key, Text value){
        try {
            writer.append(key, value);
        } catch (IOException ex) {
            Logger.getLogger(Reduce2v2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    public void cleanup(Context c){
        log.info("Finalizando o REDUCE 2.");
        
        try {
            writer.close();
        } catch (IOException ex) {
            Logger.getLogger(Reduce2v2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
}
