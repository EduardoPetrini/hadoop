/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author eduardo
 */
public class Reduce2 extends Reducer<Text, IntWritable, Text ,IntWritable>{
    
    Log log = LogFactory.getLog(Reduce2.class);
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context){
        int sum = 0;
        try {
            
            for(Iterator<IntWritable> i = values.iterator(); i.hasNext();){
                sum += i.next().get();
            }
            
            log.info("Saida do reduce2 -> key: "+key.toString());
            context.write(key, new IntWritable(sum));
        } catch (IOException | InterruptedException ex) {
            Logger.getLogger(Reduce2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
