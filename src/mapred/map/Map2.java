/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred.map;

import java.io.IOException;
import java.util.ArrayList;
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

import app.ItemTid;

/**
 * Gerar itemsets de tamanho 2.
 * @author eduardo
 */
public class Map2  extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    Log log = LogFactory.getLog(Map2.class);
    
    ItemTid invert;
    SequenceFile.Reader reader;
    int support;
    
    /**
     * Le o arquivo invertido para a memória.
     * @param context
     * @throws IOException 
     */
    @Override
    public void setup(Context context) throws IOException{
        String count = context.getConfiguration().get("count");
        String fileCachedRead = context.getConfiguration().get("fileCachedRead");
        support = Integer.parseInt(context.getConfiguration().get("support"));
        log.info("Iniciando map 2v2 count = "+count);
        log.info("Map2 support = "+support);
        log.info("Arquivo Cached = "+fileCachedRead);
        URI[] patternsFiles = context.getCacheFiles();
        
        Path path = new Path(patternsFiles[0].toString());
        
        reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(path));
        ArrayList<String> fileCached = openFile(fileCachedRead, context);
        
        for(String s : fileCached){
        	log.info(s);
        }
    }
    
    /**
     * Gera um conjunto de itemsets de tamanho dois a partir do item recebido.
     * @param item
     * @param pos
     * @param context 
     */
    public void gerarKItemSets(String item, int pos, Context context){
        
	}
    
    @Override
    public void map(LongWritable key, Text value, Context context){
    	try {
			context.write(value, new IntWritable(1));
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public ArrayList<String> openFile(String path, Context context){
    	ArrayList<String> fileCached = new ArrayList<String>();
    	try {
			
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), context.getConfiguration());
			IntWritable value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), context.getConfiguration());
			
			while (reader.next(key, value)) {
	            fileCached.add(key+" "+value);
	        }
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return fileCached;
    }
}
