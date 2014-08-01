/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author eduardo
 */
public class Reduce1BackUP extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
    
    private IntWritable result = new IntWritable();
    Log log = LogFactory.getLog(Reduce1BackUP.class);
    
    double support;
    
    @Override
    public void setup(Context c){
        /*Read conf file*/
        log.info("*********************************************************************  Starting Reduce 1 User log!!!");
    
//        BufferedReader in = null;
//        
//        try {
//            log.info("Lendo configuracao do usuario do HDFS...");
//            in = new BufferedReader(new FileReader("conf/confIn"));
//
//            String[] sp = in.readLine().split(":");
//            long lines = 0;
//            int sup = 0;
//            int conf = 0;
//            
//            if(sp.length >= 2){
//                lines = Integer.parseInt(sp[1].trim());
//            }
//            sp = in.readLine().split(":");
//            
//            if(sp.length >= 2){
//                sup = Integer.parseInt(sp[1].trim());
//            }
//            sp = in.readLine().split(":");
//            
//            if(sp.length >= 2){
//                conf = Integer.parseInt(sp[1].trim());
//            }
//
//            support = ((double)lines*(double)((double)sup/100));
//            confidence = ((double)lines*(double)((double)conf/100));
//            
//            log.info("Valor dos atributos apos o calculo: support: "+support+" confidence: "+confidence);
//            
//        } catch (FileNotFoundException ex) {
//            Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (IOException ex) {
//            Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
//        }
    }
    
    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context){
        
        int sum = 0;
        
        for (IntWritable val : values) {
          sum += val.get();
        }
        
        try {
            if(sum >= support){
                context.write(key, new IntWritable(sum));
            }
            
        } catch (IOException | InterruptedException ex) {
            Logger.getLogger(Reduce1BackUP.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    public void cleanup(Context c){
        
        /*Save support and confidence values*/
        
//        String dir = "/user/hadoop/conf/";
//        
////        checkAndDelExist(c, dir+"support");
////        checkAndDelExist(c, dir+"confidence");
//        
//        log.info("Salvando arquivos e finalizando Reduce1");
//        log.info("Valor dos atributos no cleanup: support: "+(long)support+" confidence: "+(long)confidence);
//        
//        try {
//            mo.write(new Text("support:"), new IntWritable((int)support), dir+"support");
//            mo.write(new Text("confidence:"), new IntWritable((int)confidence), dir+"confidence");
//            mo.close();
//            this.mo.close();
//        } catch (IOException | InterruptedException ex) {
//            Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
//        }
        
        
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
            Logger.getLogger(Reduce1BackUP.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
