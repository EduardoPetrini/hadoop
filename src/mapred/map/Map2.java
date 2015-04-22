/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred.map;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import app.HashTree;
import app.ItemSup;

/**
 * Gerar itemsets de tamanho 2.
 * @author eduardo
 */
public class Map2  extends Mapper<LongWritable, Text, Text, Text>{
    
    Log log = LogFactory.getLog(Map2.class);
    Text valueOut;
    Text keyOut;
    SequenceFile.Reader reader;
    ArrayList<String> blocksIds;
    HashTree hashTree;
    Integer totalPartitions;
    String splitName;
    String inputPartialName;
    ArrayList<ItemSup> lPartialItemsets;
    
    /**
     * Le o arquivo invertido para a memória.
     * @param context
     * @throws IOException 
     */
    @Override
    public void setup(Context context) throws IOException{
    	log.info("Iniciando Map 2");
    	totalPartitions = Integer.parseInt(context.getConfiguration().get("totalPartitions"));
    	inputPartialName = context.getConfiguration().get("outputPartialName");
        String count = context.getConfiguration().get("count");
        
        blocksIds = new ArrayList<String>();
    	for(int i = 1; i <= totalPartitions; i++){
    		blocksIds.add(context.getConfiguration().get("blockId"+i));//Id da partição é o offset do bloco
    	}
    	
    	/*No método 'map', ao identificar que a partição atual será processada, le seus itemsets para memória*/
//        URI[] patternsFiles = context.getCacheFiles();
        
//        Path path = new Path(patternsFiles[0].toString());
//        
//        openFile(fileCachedRead, context);
        
    }
    
    public boolean checkPartition(){
    	
    	for(String ids: blocksIds){
    		System.out.println("Verificando se a partição atual será processada: "+splitName+" == "+ids);
    		if(ids.equalsIgnoreCase(splitName)){
    			return true;
    		}
    	}
    	
    	return false;
    }
    
    @Override
    public void map(LongWritable key, Text value, Context context){
    	//key é o offset, id do bloco/partição
    	splitName = String.valueOf(key.get());
    	System.out.println("Id da partição: "+splitName);
    	//Verificar se é uma partição a ser processada
    	
    	if(checkPartition()){
    		//A partição atual será processada
    		//Executar o map da fase 2
    		
    		openFile(context);//Ler o arquivo da partição para a memória
    		//Para cada lPartialItemset, contar o suporte em Di (value)
    		int supportLocal;
    		valueOut = new Text();
    		keyOut = new Text();
    		for(ItemSup item: lPartialItemsets){
    			supportLocal = count(value.toString().split("\n"), item.getItemset().toString());
    			
    			try{
    				valueOut.set(String.valueOf(item.getSupport())+"+"+supportLocal);
    				keyOut.set(item.getItemset());
    				context.write(keyOut, valueOut);
    			}catch(IOException | InterruptedException e){
    				e.printStackTrace();
    			}
    		}
    	}
    	
    }
    
    /**
     * 
     * @param transactions
     * @param itemset
     * @return
     */
    public int count(String[] transactions, String itemset){
    	int count = 0;
    	int i,j;
    	String[] tSplit;
    	String[] itemsetSplit = itemset.split(" ");
    	for_trans:
    	for(String transaction: transactions){
    		i = 0;
    		j = 0;
    		tSplit = transaction.split(" ");
    		
    		while(true){
    			if(itemsetSplit[i].equals(tSplit[j])){
					if(++i == itemsetSplit.length || 
							++j == tSplit.length-1){
						count++;
						continue for_trans;
					}
					
				}else{
					if(++j == tSplit.length-1){
						continue for_trans;
					}
				}
    		}
    	}
    	
    	return count;
    }
    
    /**
     * 
     * @param context
     */
    public void openFile(Context context){
    	Path path = new Path(inputPartialName+splitName); 

    	try {
    		System.out.println("Lendo a partição '"+path.getName()+"' para a memória!");
    		reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(path));
    		lPartialItemsets = new ArrayList<ItemSup>();
			
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), context.getConfiguration());
			IntWritable value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), context.getConfiguration());
			
			while (reader.next(key, value)) {
				System.out.println("Add Key: "+key.toString());
				lPartialItemsets.add(new ItemSup(key.toString(), value.get()));//Adicionar direto na prefix tree
			}
 
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
    }
}
