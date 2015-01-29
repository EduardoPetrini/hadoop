/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred.map;

import java.io.IOException;
import java.net.URI;
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

import app.PrefixTree;

/**
 * Gerar itemsets de tamanho k, k+1 e k+3 em uma única instância Map/Reduce.
 * @author eduardo
 */
public class Map3  extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    Log log = LogFactory.getLog(Map3.class);
    IntWritable countOut = new IntWritable(1);
    SequenceFile.Reader reader;
    ArrayList<String> fileCached;
    ArrayList<String> itemsetAux;
    PrefixTree prefixTree;
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
        String kStr = context.getConfiguration().get("k");
        k = Integer.parseInt(kStr);
        
        log.info("Iniciando map 3 count = "+count);
        log.info("Arquivo Cached = "+fileCachedRead);
        URI[] patternsFiles = context.getCacheFiles();
        
        Path path = new Path(patternsFiles[0].toString());
        
        reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(path));
        openFile(fileCachedRead, context);
        
        //Gerar combinações dos itens de acordo com k, k+1 e k+2
        
        prefixTree = new PrefixTree(0);
        itemsetAux = new ArrayList<String>();
        
        log.info("K is "+k);
        String itemsetC;
        System.out.println("\nPrimeiro passo!");
        prefixTree.printStrArray(fileCached);
        
        log.info(context.getStatus());
        if(fileCachedRead != null && fileCached.size() > 0){
        	if(fileCached.get(fileCached.size()-1).split(" ").length < k-1){
	        	log.info("Itemsets é menor do que k");
	        	prefixTree.printStrArray(fileCached);
	        	System.exit(0);
        	}
        }else{
        	log.info("Arquivo do cache distribuído é vazio!");
        	System.exit(0);
        }
        for (int i = 0; i < fileCached.size(); i++){
        	for (int j = i+1; j < fileCached.size(); j++){
        		String[] itemA = fileCached.get(i).split(" ");
        		String[] itemB = fileCached.get(j).split(" ");
        		if(isSamePrefix(itemA, itemB, i, j)){
        			itemsetC = combine(itemA, itemB);
        			itemsetAux.add(itemsetC);
        			System.out.println(itemsetC+" no primeiro passo");
        			//Building HashTree
        			prefixTree.add(prefixTree, itemsetC.split(" "), 0);
        		}
        	}
        }
        prefixTree.printPrefixTree(prefixTree);
        System.out.println("\nSegundo passo!");
        prefixTree.printStrArray(itemsetAux);
        
        log.info(context.getStatus());
        k++;
        log.info("K is "+k);
        fileCached.clear();
        for (int i = 0; i < itemsetAux.size(); i++){
        	for (int j = i+1; j < itemsetAux.size(); j++){
        		String[] itemA = itemsetAux.get(i).split(" ");
        		String[] itemB = itemsetAux.get(j).split(" ");
        		if(isSamePrefix(itemA, itemB, i, j)){
        			itemsetC = combine(itemA, itemB);
        			fileCached.add(itemsetC);
        			System.out.println(itemsetC+" no segundo passo");
        			//Building HashTree
        			prefixTree.add(prefixTree, itemsetC.split(" "), 0);
        		}
        	}
        }
        prefixTree.printPrefixTree(prefixTree);
        System.out.println("\nTerceiro passo!");
        prefixTree.printStrArray(fileCached);
        
        log.info(context.getStatus());
        k++;
        log.info("K is "+k);
        itemsetAux.clear();
        for (int i = 0; i < fileCached.size(); i++){
        	for (int j = i+1; j < fileCached.size(); j++){
        		String[] itemA = fileCached.get(i).split(" ");
        		String[] itemB = fileCached.get(j).split(" ");
        		if(isSamePrefix(itemA, itemB, i, j)){
        			itemsetC = combine(itemA, itemB);
        			System.out.println(itemsetC+" no terceiro passo");
        			//Building HashTree
        			prefixTree.add(prefixTree, itemsetC.split(" "), 0);
        		}
        	}
        }
        prefixTree.printStrArray(itemsetAux);
        System.out.println("Fim do setup, inicia função map");
        prefixTree.printPrefixTree(prefixTree);
        
        log.info(context.getStatus());
    }
    
    public boolean isSamePrefix(String[] itemA, String[] itemB, int i, int j){
    	if(k == 2) return true;
    	for(int a = 0; a < k -2; a++){
            if(!itemA[a].equals(itemB[a])){
            	System.out.println("Não é o mesmo prefixo: "+itemA[a]+" != "+itemB[a]);
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
    
    public void subset(String[] transaction, PrefixTree pt, int i, StringBuilder sb, Context context){
    	if(i >= transaction.length){
			return;
		}
		int index = pt.getPrefix().indexOf(transaction[i]);
		
		if(index == -1){
			System.out.println("Não achou :( "+sb.toString());
			return;
		}else{
			if(i == transaction.length-1){
				sb.append(transaction[i]);
				System.out.println("Achou :) "+sb.toString());
				
				//Manda pro reduce
				try {
					context.write(new Text(sb.toString()), new IntWritable(1));
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return;
			}else{
				sb.append(transaction[i]).append(" ");
				i++;
				if(pt.getPrefixTree().isEmpty()){
					System.out.println("Não achou :'( "+sb.toString());
					return;
				}else{
					subset(transaction, pt.getPrefixTree().get(index), i, sb ,context);
				}
			}
		}
    }
    
    @Override
    public void map(LongWritable key, Text value, Context context){
    	
		//Aplica a função subset e envia o itemset para o reduce
    	StringBuilder sb = new StringBuilder();
    	String[] transaction = value.toString().split(" ");
    	if(transaction.length >= k-2){
    		subset(transaction, prefixTree, 0, sb , context);
    	}
    }
    
    public ArrayList<String> openFile(String path, Context context){
    	fileCached = new ArrayList<String>();
    	try {
			
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), context.getConfiguration());
			IntWritable value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), context.getConfiguration());
			
			while (reader.next(key, value)) {
				System.out.println("Add Key: "+key.toString());
	            fileCached.add(key.toString());
	        }
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return fileCached;
    }
}
