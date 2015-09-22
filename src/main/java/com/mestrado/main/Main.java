/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.main;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import main.java.com.mestrado.hadoop.inputformat.WholeInputFormat;
import main.java.com.mestrado.mapred.map.Map1;
import main.java.com.mestrado.mapred.map.Map2;
import main.java.com.mestrado.mapred.reduce.Reduce1;
import main.java.com.mestrado.mapred.reduce.Reduce2;
import main.java.com.mestrado.utils.CountItemsets;
import main.java.com.mestrado.utils.MrUtils;

/**
 *
 * @author eduardo
 */
public class Main {

    private Log log = LogFactory.getLog(Main.class);
    public static int countDir;
    private static int timeTotal;
    public static double supportRate = 0.005;
    public static String support;
    private int k = 1;
    public static int totalBlockCount;
    public static String user = "/user/hdp/";
    public static String inputEntry = "input/";
    public static String inputFileName = "";
    public static String clusterUrl = "hdfs://master/";
    public static long totalTransactionCount;
    public ArrayList<String> blocksIds;
    public String outputPartialName = user+"partitions-fase-1/partition";
    public static ArrayList<String> seqFilesNames;
    public static int NUM_REDUCES = 1;
    public static String NUM_BLOCK = "0";
    
    public Main() {
        countDir = 0;
        timeTotal = 0;
    }
 
    /**
     * 
     */
    public void job1(){
        
        Configuration c = new Configuration();
        Job job = null;
        try {
            job = Job.getInstance(c);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        job.getConfiguration().set("fs.defaultFS", "hdfs://master/");
        job.setJobName("ImrApriori Fase 1");
        
        job.setJarByClass(Main.class);
        
        job.setMapperClass(Map1.class);
//        job.setCombinerClass(Combiner1.class);
        job.setReducerClass(Reduce1.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(WholeInputFormat.class);
        
        job.getConfiguration().set("count", String.valueOf(Main.countDir));
        job.getConfiguration().set("support", support);
        job.getConfiguration().set("supportPercentage", String.valueOf(supportRate));
        job.getConfiguration().set("outputPartialName", outputPartialName);
        job.getConfiguration().set("totalMaps", String.valueOf(totalBlockCount));
        job.getConfiguration().set("totalTransactions", String.valueOf(totalTransactionCount));
       
        for(int i = 1; i <= this.blocksIds.size(); i++){
        	job.getConfiguration().set("blockId"+i, this.blocksIds.get(i-1).replace("partition", ""));
        }
        
        job.setNumReduceTasks(NUM_REDUCES);
        
        try {
        	FileInputFormat.setInputPaths(job, new Path(user+"input"));
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        FileOutputFormat.setOutputPath(job, new Path(user+"output"+Main.countDir));
        
        try {
//            long ini = System.currentTimeMillis();
            int st = (job.waitForCompletion(true) ? 0 : 1);
//            long fim = System.currentTimeMillis();
            long ini = job.getStartTime();
            long fim = job.getFinishTime();
            long t = fim - ini;
            System.out.println("Tempo do ImrApriori Fase 1: "+((double)t/1000));
            
            timeTotal += t;
            if(st == 1){
                System.exit(st);
            }
            
        } catch (InterruptedException | ClassNotFoundException | IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * 
     */
    public void job2(){
        
        Configuration c = new Configuration();
        
        Job job = null;
        try {
            job = Job.getInstance(c);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
          job.getConfiguration().set("fs.defaultFS", "hdfs://master/");
        job.setJobName("ImrApriori Fase 2");
        
        job.setJarByClass(Main.class);
        
        job.setMapperClass(Map2.class);
//        job.setCombinerClass(Reduce2.class);
        job.setReducerClass(Reduce2.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(WholeInputFormat.class);
        
        job.getConfiguration().set("count", String.valueOf(Main.countDir));
        job.getConfiguration().set("support", support);
        job.getConfiguration().set("totalPartitions", String.valueOf(blocksIds.size()));
        job.getConfiguration().set("outputPartialName", outputPartialName);
        job.getConfiguration().set("totalTransactions", String.valueOf(totalTransactionCount));
        job.getConfiguration().set("supportPercentage", String.valueOf(supportRate));
        
        for(int i = 1; i <= this.blocksIds.size(); i++){
        	job.getConfiguration().set("blockId"+i, this.blocksIds.get(i-1));
        	try {
    			job.addCacheFile(new URI(user+"partitions-fase-1/"+this.blocksIds.get(i-1)));
        	} catch (URISyntaxException ex) {
        		Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        	}
        }
          
        job.setNumReduceTasks(NUM_REDUCES);
        
        
        try {
            FileInputFormat.setInputPaths(job, new Path(user+"input"));
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        FileOutputFormat.setOutputPath(job, new Path(user+"output"+Main.countDir));
        try {
//            long ini = System.currentTimeMillis();
            int st = (job.waitForCompletion(true) ? 0 : 1);
//            long fim = System.currentTimeMillis();
            long ini = job.getStartTime();
            long fim = job.getFinishTime();
            long t = fim - ini;
            System.out.println("Tempo do ImrApriori Fase 2: "+((double)t/1000));
            
            timeTotal += t;
            if(st == 1){
                System.exit(st);
            }
            
        } catch (InterruptedException | ClassNotFoundException | IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void endTime(){
        double seg = ((double)timeTotal/1000);
    	StringBuilder sb = new StringBuilder();
    	SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    	sb.append("#\n");
    	sb.append("DATA=").append(format.format(new Date())).append("\n");
    	sb.append("TEMPO=").append(seg).append("\n");
    	sb.append("ITEMSETS=");
    	sb.append(CountItemsets.countItemsets()).append("\n");
    	MrUtils.saveTimeLog(sb.toString());
    }
    
    public static void main(String[] args) throws IOException {
        Main m = new Main();
//        System.out.println(m.checkOutput(user+"output1"));
      
        MrUtils.initialConfig(args);//Dentre outras coisas, define a quantidade total de transações
        m.blocksIds = MrUtils.extractBlocksIds();
        MrUtils.createIfNotExistOrClean(m.outputPartialName);
        MrUtils.printConfigs(m);
        
        MrUtils.delOutDirs(user);
        Main.countDir++;
        m.job1();
        
        m.blocksIds = MrUtils.getPartitions(m.outputPartialName);
        if(m.blocksIds.size() == 0){
        	endTime();
        	System.exit(0);
        }
        //configurar o suporte global
        MrUtils.configGlobalSupporte();
//        MrUtils.getAllSequenceFilesNames();
        
		Main.countDir++;
		m.job2();
		
		endTime();
    }
}
