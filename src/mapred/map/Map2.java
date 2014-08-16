/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred.map;

import app.ItemTid;

import java.io.File;
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
public class Map2  extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    Log log = LogFactory.getLog(Map2.class);
    
    ItemTid invert;
    
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
        int countInt = Integer.parseInt(count) -1;
        log.info("Iniciando map 2v2");
        log.info("Map2 support = "+support);
        
        File file = new File("/user/eduardo/output"+countInt);
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
        
    }
}
