/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred;

import app.ItemTid;
import app.Main;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

/**
 *
 * @author eduardo
 */
public class Map3v2 extends Mapper<LongWritable, Text, Text, Text>{
    
    Log log = LogFactory.getLog(Map3v2.class);
    
    ItemTid invert;
    ArrayList<String> keys;
    SequenceFile.Reader reader;
    int support;
    int k;
    int loop;
    
    /**
     * Le o arquivo invertido para a memória.
     * @param c
     * @throws IOException 
     */
    @Override
    public void setup(Mapper.Context c) throws IOException{
        String count = c.getConfiguration().get("count");
        support = Integer.parseInt(c.getConfiguration().get("support"));
        
        log.info("Iniciando map 3v2");
        log.info("Map3v2 support = "+support);
        
        invert = new ItemTid(); 
        keys = new ArrayList();
        
        reader = new SequenceFile.Reader(c.getConfiguration(), SequenceFile.Reader.file(new Path("/user/hadoop/invert/invertido"+(Integer.parseInt(count)-1))));
        
        Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), c.getConfiguration());
        Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), c.getConfiguration());
        
        String[] values;
        ArrayList<LongWritable> tids;
        
        while (reader.next(key, value)) {
           
            keys.add(key.toString());
            
            values = value.toString().split(",");
            values[0] = values[0].replace("[", "");
            values[values.length-1] = values[values.length-1].replace("]", "");
            
            tids = new ArrayList();
            
            for(String v: values){
                if(!v.isEmpty()){
                    tids.add(new LongWritable(Integer.parseInt(v.trim())));
                }
            }
            
            invert.put(key.toString(), tids);
           
        }
        
        IOUtils.closeStream(reader);
        
        log.info("Arquivo invertido carregado na memória! Keys: "+keys.size()+" Invert: "+invert.size());
        
    }
    
    /**
     * 
     * @param kitem
     * @param v
     * @param context 
     */
    public void sendToReduce(String kitem, ArrayList<LongWritable> v, Context context){
        
        Text key = new Text(kitem);
        Text value = new Text(v.toString());
        
        
        try {
            context.write(key, value);
        } catch (IOException | InterruptedException ex) {
            Logger.getLogger(Map3v2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     * 
     * @param kitem
     * @return 
     */
    public String getPrefix(String kitem){
        
        String[] spkitem = kitem.split("-");
        StringBuilder sb = new StringBuilder();
        
        for (int i = 0; i < spkitem.length-1; i++) {
            
            sb.append(spkitem[i]).append("-");
        }
        
        k = spkitem.length;
        return sb.toString().trim();
    }
    
    /**
     * 
     * @param kitem
     * @param pos
     * @param localKeys
     * @param localInvert
     * @param context 
     */
    public void gerarKItemSets(String kitem, int pos, ArrayList<String> localKeys, ItemTid localInvert, Context context){
  
        String prefix = getPrefix(kitem);
//        log.info("Item: "+kitem+" prefix: "+prefix);

        String kitem2;
        StringBuilder novoItem;
        ArrayList<LongWritable> aux;

        ArrayList<String> lKeys = new ArrayList();
        ItemTid arqInv = new ItemTid();
        
        int i = pos;
        
        kitem2 = localKeys.get(i);
        
        while(kitem2.startsWith(prefix)){
            
            /*Checa suporte e poda*/
            /*Efetuar a combinação*/
            
//            log.info(kitem+" combina com "+kitem2);
            
            if((aux = ItemTid.intersection2(localInvert.get(kitem), localInvert.get(kitem2))).size() > support){
                novoItem = new StringBuilder();

                novoItem.append(kitem).append("-");
                novoItem.append(kitem2.split("-")[k-1]);

                arqInv.put(novoItem.toString(), aux);
                lKeys.add(novoItem.toString());
                
                /*Enviar para o reduce*/
                sendToReduce(novoItem.toString(), aux, context);
               
            }
            
            i++;
            if(i < localKeys.size()){
                
                kitem2 = localKeys.get(i);
            }else{
                break;
            }
        }
        
        /*Chamar a função novamente para k+1*/
        
        if(loop < 2 && !lKeys.isEmpty() && lKeys.size() > 1){
            loop++;
            gerarKItemSets(lKeys.get(0), 1, lKeys, arqInv, context);
        }
    }
    
    /**
     * Para cada k-itemset obtido do disco, combinar com os itens do arquivo invertido respeitando prefixo.
     * @param tid
     * @param value
     * @param context
     * @throws IOException 
     */
    @Override
    public void map(LongWritable tid, Text value, Context context) throws IOException{
        String[] input = value.toString().split("\\s+");//Posição 0 item, posição 1 tids
        
        int pos = keys.indexOf(input[0]);
        
        if(pos < keys.size()-1 && pos >= 0){
            loop = 1;
            gerarKItemSets(input[0], pos+1, this.keys, this.invert, context);
        }
        
    }
    
    @Override
    public void cleanup(Context c){
        log.info("Finalizando o Map3v2!");
    }
}
