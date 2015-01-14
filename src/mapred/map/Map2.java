/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

import app.ItemTid;

/**
 * Gerar itemsets de tamanho 2.
 * @author eduardo
 */
public class Map2  extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    Log log = LogFactory.getLog(Map2.class);
    IntWritable countOut = new IntWritable(1);
    SequenceFile.Reader reader;
    HashMap<String, Integer> fileCached;
    int k;
    /**
     * Le o arquivo invertido para a memória.
     * @param context
     * @throws IOException 
     */
    @Override
    public void setup(Context context) throws IOException{
        String count = context.getConfiguration().get("count");
        String fileCachedRead = context.getConfiguration().get("fileCachedRead");
        k = Integer.parseInt(count);
        
        log.info("Iniciando map 2v2 count = "+count);
        log.info("Arquivo Cached = "+fileCachedRead);
        URI[] patternsFiles = context.getCacheFiles();
        
        Path path = new Path(patternsFiles[0].toString());
        
        reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(path));
        openFile(fileCachedRead, context);
        
        //Gerar combinações dos itens de acordo com k
        
        String[] keys = (String[]) fileCached.keySet().toArray();
        
        for (int i = 0; i < keys.length; i++){
        	for (int j = i+1; j < keys.length; j++){
        		String[] itemA = keys[i].split(" ");
        		String[] itemB = keys[j].split(" ");
        		if(isSamePrefix(itemA, itemB, i, j)){
        			System.out.println(combine(itemA, itemB));
        		}
        	}
        }
    }
    
    public boolean isSamePrefix(String[] itemA, String[] itemB, int i, int j){
    	if(k == 2) return true;
    	for(int a = 0; a < k -2; a++){
            if(itemA[a] != itemB[a]){
                return false;
            }
        }
        
    	return true;
    }
    
    public String combine(String[] itemA, String[] itemB){
        StringBuilder sb = new StringBuilder();
        
        for(int i = 0; i < itemA.length; i++){
            sb.append(itemA[i]).append(" ");
        }
        sb.append(itemB[itemB.length-1]);
        return sb.toString();
    }
    
    public boolean isFrequent(String item, int x){
    	System.out.println(x+" Item : "+item);
    	if(fileCached.get(item) != null){
    		return true;
    	}
    	System.out.print(" - Não está no cache!");
    	return false;
    }
    
    /**
     * Gera um conjunto de itemsets de tamanho dois a partir do item recebido.
     * @param item
     * @param pos
     * @param context 
     */
    public void generateKItemSets(String[] transaction, Context context){
        /*Verificar se o item é frequente*/
    	
    	int i = 0;
    	int j;
    	int size = transaction.length;
    	System.out.println(new ArrayList(Arrays.asList(transaction)));
    	if(size >= k){
    		while(i < size && isFrequent(transaction[i], 1)){//utilizar substring. Se o item não for frequente substituí-lo por -1 para não chamar o 'isFrequent' a partir da segunda iteração.
	    		j = i+1;
	    		while(j < size && isFrequent(transaction[j], 2)){
					
	    			try {
						context.write(new Text(transaction[i]+" "+transaction[j] ), countOut);
					} catch (IOException | InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	    			j++;
				}
	    		i++;
	    		if(i == size-1) break;
	    	}
    	}
	}
    
    @Override
    public void map(LongWritable key, Text value, Context context){
    	
		generateKItemSets(value.toString().split(" "), context);
		
    }
    
    public HashMap<String, Integer> openFile(String path, Context context){
    	fileCached = new HashMap<String, Integer>();
    	try {
			
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), context.getConfiguration());
			IntWritable value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), context.getConfiguration());
			
			while (reader.next(key, value)) {
	            fileCached.put(key.toString(), value.get());
	        }
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return fileCached;
    }
}
