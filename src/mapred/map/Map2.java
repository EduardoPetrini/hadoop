/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred.map;

import app.ItemTid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import main.Main;

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

/**
 * Gerar itemsets de tamanho 2.
 * @author eduardo
 */
public class Map2  extends Mapper<LongWritable, Text, Text, Text>{
    
    Log log = LogFactory.getLog(Map2.class);
    
    ItemTid invert;
    ArrayList<String> keys;
    SequenceFile.Reader reader;
    int support;
    
    /**
     * Le o arquivo invertido para a memória.
     * @param c
     * @throws IOException 
     */
    @Override
    public void setup(Mapper.Context c) throws IOException{
        String count = c.getConfiguration().get("count");
        support = Integer.parseInt(c.getConfiguration().get("support"));
        
        log.info("Iniciando map 2v2");
        log.info("Map2 support = "+support);
        
        invert = new ItemTid(); 
        keys = new ArrayList();
        
        reader = new SequenceFile.Reader(c.getConfiguration(), SequenceFile.Reader.file(new Path("/user/eduardo/invert/invertido"+(Integer.parseInt(count)-1))));
        
        IntWritable key = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(), c.getConfiguration());
        Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), c.getConfiguration());
        
        String[] values;
        ArrayList<LongWritable> tids;
        
        while (reader.next(key, value)) {
           
            keys.add(String.valueOf(key));
            
            values = value.toString().split(",");
            values[0] = values[0].replace("[", "");
            values[values.length-1] = values[values.length-1].replace("]", "");
            
            tids = new ArrayList();
            
            for(String v: values){
                tids.add(new LongWritable(Integer.parseInt(v.trim())));
            }
            
            invert.put(String.valueOf(key), tids);
           
        }
        
        /*Imprime chaves*/
//        for(IntWritable k: keys){
//            log.info("Chaves: "+k.get());
//        }
        
        IOUtils.closeStream(reader);
        
        log.info("Arquivo invertido carregado na memória! Keys: "+keys.size()+" Invert: "+invert.size());
        
    }
    
    /**
     * Gera um conjunto de itemsets de tamanho dois a partir do item recebido.
     * @param item
     * @param pos
     * @param context 
     */
    public void gerar2ItemSets(String item, int pos, Context context){
        
        StringBuilder item2;
        
        ArrayList<LongWritable> itemTids = invert.get(item);
        
        if(itemTids == null){
            log.info("O valor é nulo para o item: "+item);
            log.info("Verificar se ele ocorre no arquivo invertido e na saída do MR 1.");
            return;
        }
        
        ArrayList<LongWritable> itemTids2;
        ArrayList<LongWritable> aux;
        
        Text key = new Text();
        Text value = new Text();
        
        for (int i = pos; i < keys.size(); i++) {
            
            itemTids2 = invert.get(keys.get(i));
            
            if((aux = ItemTid.intersection2(itemTids, itemTids2)).size() > support){
                item2 = new StringBuilder();
                item2.append(item);
                
                /*Define o par de itens*/
                item2.append("-");
                item2.append(keys.get(i));
                
                /*Converte a interseção para String*/
//                for(LongWritable l: aux){
//                    value2.append(String.valueOf(l)).append(":");
//                }
                
                key.set(item2.toString());
                value.set(aux.toString());
                
                /*Envia o novo par key / value para o reduce*/
                try {
                    context.write(key, value);
                } catch (IOException | InterruptedException ex) {
                    Logger.getLogger(Map2.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            
            itemTids2 = null;
            item2 = null;
            aux = null;
//            System.gc();
        }
    }
    
    @Override
    public void map(LongWritable key, Text value, Context context){
        
        String[] input = value.toString().split("\\s+");//Posição 0 item, posição 1 tids
        int pos = keys.indexOf(input[0]);
        
        if(pos >= 0){
            log.info("Item: "+input[0]);
            gerar2ItemSets(input[0], pos+1, context);
        }
    }
}
