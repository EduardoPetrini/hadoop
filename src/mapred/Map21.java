package mapred;


import app.ItemTid;
import java.io.IOException;
import java.util.ArrayList;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import mapred.Map2;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author eduardo
 */
public class Map21 extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    Log log = LogFactory.getLog(Map21.class);
    
    ItemTid invert;
    SequenceFile.Reader reader;
    int minSup = 2;
    Text outKey = new Text();
    IntWritable v = new IntWritable();
    
    
    @Override
    public void setup(Mapper.Context c) throws IOException{
        
        log.info("Iniciando map 21");
        invert = new ItemTid(); 
        
        reader = new SequenceFile.Reader(c.getConfiguration(), SequenceFile.Reader.file(new Path("/user/hadoop/invert/invertido")));
        
        IntWritable key = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(), c.getConfiguration());
        Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), c.getConfiguration());
        
        String[] values;
        ArrayList<LongWritable> tids;
        
        log.info("Lendo o arquivo invertido!");
        
        while (reader.next(key, value)) {
           
            values = value.toString().split(":");
            tids = new ArrayList();
            
            for(String v: values){
                tids.add(new LongWritable(Integer.parseInt(v)));
            }
            
            invert.put(String.valueOf(key.get()), tids);
           
        }
                
        log.info("Dados carregados na memória!");
        
        IOUtils.closeStream(reader);
        reader.close();
        
    }
    
    @Override
    public void map(LongWritable key, Text value, Context context){
        
        /*Cada linha do conjunto de transações original*/
        String[] items = value.toString().split(" ");
        ArrayList<String> freqItens = new ArrayList();
        int count = 0;
        
        for(String i: items){
            
            /*Testa o suporte do item*/
            if(invert.getItemSupport(i) >= minSup){
                count++;
                freqItens.add(i);
            }
            
        }
        
        if(!freqItens.isEmpty()){
            
            /*  Efetua as combinações com os itens frequentes de uma mesma transação
                k = 1
                count = número de itens frequentes
            */
            log.info("FreqItens nao e vazio, seu tamanho: "+count);
            
            ArrayList<String> itens = new ArrayList();
            combineItens(freqItens, count, 1, itens, context);
            
        }
        
    }

    /**
     * 
     * @param freqItens
     * @param count
     * @param k
     * @param item
     * @param context 
     */
    private void combineItens(ArrayList<String> freqItens, int count, int k, ArrayList<String> item, Context context) {
        
        StringBuilder sb;
        for (int i = (k-1); i < count; i++) {
            
            if(i == 0){
                
                item.add(freqItens.get(i));
                
            }else{
                item.add(freqItens.get(i));
                
                int ini = 1;
                
                
                sb = new StringBuilder();
                
                for(String s: item){
                    if(ini == 1){
                        sb.append(s);
                    }else{
                        sb.append(" ");
                        sb.append(s);
                    }
                }
                
                try {
                    outKey.set(sb.toString());
                    v.set(1);
                    context.write(outKey, v);
                } catch (IOException | InterruptedException ex) {
                    Logger.getLogger(Map21.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            
            
            if(i < count-1){
                combineItens(freqItens, count, k+1, item, context);
            }
            
            int total = item.size();
            if(total > 0){
                item.remove(total-1);
            }
        }
    }
}
