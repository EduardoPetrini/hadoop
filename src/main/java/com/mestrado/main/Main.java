/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.main;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import main.java.com.mestrado.hadoop.inputformat.WholeInputFormat;
import main.java.com.mestrado.mapred.map.Map1;
import main.java.com.mestrado.mapred.map.Map2;
import main.java.com.mestrado.mapred.map.Map3;
import main.java.com.mestrado.mapred.reduce.Reduce1;
import main.java.com.mestrado.mapred.reduce.Reduce2;
import main.java.com.mestrado.mapred.reduce.Reduce3;
import main.java.com.mestrado.utils.CountItemsets;
import main.java.com.mestrado.utils.MrUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author eduardo
 */
public class Main {

    public static int countDir;
    private static int timeTotal;
    public static double supportRate = 0.01;
    public static String support;
    public static int k = 1;
    public static String user = "/user/hdp/";
    public static String inputEntry = "input/";
    public static String inputFileName = "";
    public static String clusterUrl = "hdfs://master/";
    public static String fileCached = user+"outputCached/outputMR";
    public static String fileCachedDir = user+"outputCached/";
    public static long totalTransactionCount;
    public static double earlierTime;
    public static ArrayList<String> seqFilesNames;
    public static String NUM_BLOCK = "2b";
    public static int NUM_REDUCES = 2;
    private static ArrayList<String> timeByStep;
    private static ArrayList<String> itemsetsByStep;
    private static DecimalFormat format = new DecimalFormat("#.000");
    
    public Main() {
        countDir = 0;
        timeTotal = 0;
        timeByStep = new ArrayList<String>();
        itemsetsByStep = new ArrayList<String>();
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
         job.getConfiguration().set("fs.defaultFS", clusterUrl);
        job.setJobName("AprioriDpc Fase 1");
        
        job.setJarByClass(Main.class);
        
        job.setMapperClass(Map1.class);
//        job.setCombinerClass(Combiner1.class);
        job.setReducerClass(Reduce1.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(WholeInputFormat.class);
        
        job.getConfiguration().set("count", String.valueOf(Main.countDir));
        job.getConfiguration().set("support", String.valueOf(support));
        job.getConfiguration().set("fileCached", fileCached+(Main.countDir));
        
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
            String itemsetStep = MrUtils.countItemsetInOutput(user+"output"+Main.countDir);
            System.out.println("Tempo AprioriDpc Fase 1: "+((double)t/1000)+" Itemsets: "+itemsetStep);
            timeByStep.add(String.valueOf(format.format(((double)t/1000)))+":"+Main.countDir);
            timeTotal += t;
            if(st == 1){
                System.exit(st);
            }
            itemsetsByStep.add(itemsetStep + ":" + Main.countDir);
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
         job.getConfiguration().set("fs.defaultFS", clusterUrl);
        job.setJobName("AprioriDpc Fase 2");
        
        job.setJarByClass(Main.class);
        
        job.setMapperClass(Map2.class);
//        job.setCombinerClass(Reduce2.class);
        job.setReducerClass(Reduce2.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(WholeInputFormat.class);
        
        k++;
        job.getConfiguration().set("count", String.valueOf(Main.countDir));
        job.getConfiguration().set("support", String.valueOf(support));
        job.getConfiguration().set("k", String.valueOf(k));
        job.getConfiguration().set("lksize", String.valueOf(seqFilesNames.size()));
        for(int i = 0; i < seqFilesNames.size(); i++){
        	job.getConfiguration().set("fileCachedRead"+i, seqFilesNames.get(i));
        	try {
        		job.addCacheFile(new URI(seqFilesNames.get(i)));
        	} catch (URISyntaxException ex) {
        		Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        	}
        }
        job.getConfiguration().set("fileCachedWrited", fileCached+(Main.countDir));
        
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
            earlierTime = ((double)t/1000);
            timeByStep.add(String.valueOf(format.format(((double)t/1000)))+":"+Main.countDir);
            String itemsetStep = MrUtils.countItemsetInOutput(user+"output"+Main.countDir);
            System.out.println("Tempo AprioriDpc Fase 2: "+earlierTime+" Itemsets: "+itemsetStep);
            
            timeTotal += t;
            if(st == 1){
                System.exit(st);
            }
            itemsetsByStep.add(itemsetStep+ ":" + Main.countDir);
        } catch (InterruptedException | ClassNotFoundException | IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     * 
     */
    public void job3(){
        
        Configuration c = new Configuration();
        
        Job job = null;
        try {
            job = Job.getInstance(c);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
         job.getConfiguration().set("fs.defaultFS", clusterUrl);
        job.setJobName("AprioriDpc Fase 3");
        
        job.setJarByClass(Main.class);
        
        job.setMapperClass(Map3.class);
//        job.setCombinerClass(Reduce2.class);
        job.setReducerClass(Reduce3.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(WholeInputFormat.class);
        
        job.getConfiguration().set("count", String.valueOf(Main.countDir));
        job.getConfiguration().set("support", String.valueOf(support));
        job.getConfiguration().set("k", String.valueOf(k));
        job.getConfiguration().set("lksize", String.valueOf(seqFilesNames.size()));
        for(int i = 0; i < seqFilesNames.size(); i++){
        	job.getConfiguration().set("fileCachedRead"+i, seqFilesNames.get(i));
        	try {
        		job.addCacheFile(new URI(seqFilesNames.get(i)));
        	} catch (URISyntaxException ex) {
        		Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        	}
        }
        job.getConfiguration().set("fileCachedWrited", fileCached+(Main.countDir));
        job.getConfiguration().set("earlierTime", String.valueOf(earlierTime));
        System.out.println("AprioriDpc Fase 3 - CountDir: "+Main.countDir);
        
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
            earlierTime = ((double)t/1000);
            timeByStep.add(String.valueOf(format.format(((double)t/1000)))+":"+Main.countDir);
            
            timeTotal += t;
            if(st == 1){
                System.exit(st);
            }
            long item1 = Long.valueOf(MrUtils.countItemsetInOutput(user+"output"+Main.countDir));
            long item2 = Long.valueOf(MrUtils.countItemsetInOutput("outputMR"+(Main.countDir)));
            		
    		itemsetsByStep.add(String.valueOf(item1+item2)+":"+Main.countDir);
    		System.out.println("Tempo AprioriDpc Fase 3: "+earlierTime+" Itemsets: "+(item1+item2));
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
    	sb.append(createStringByArray());
    	sb.append("ITEMSETS=");
    	sb.append(CountItemsets.countItemsets()).append("\n");
        MrUtils.saveTimeLog(sb.toString());
    }
    
private static String createStringByArray(){
    	
    	StringBuilder sb = new StringBuilder();
    	String[] t;
    	sb.append("-----TIME BY STEP-----\n");
    	for(String s: timeByStep){
    		t = s.split(":");
    		sb.append("Fase ").append(t[1]);
			sb.append(": ").append(t[0]).append("\n");
    	}
    	sb.append("\n-----ITEMSETS BY STEP-----\n");
    	for(String s: itemsetsByStep){
    		t = s.split(":");
    		sb.append("Fase ").append(t[1]);
			sb.append(": ").append(t[0]).append("\n");
    	}
    	
    	return sb.toString();
    }

    public static void main(String[] args) throws IOException {
        Main m = new Main();
        MrUtils.delOutDirs(user);
        MrUtils.initialConfig(args);
        
        Main.countDir++;
        MrUtils.printConfigs(m);
        
        m.job1();
        if(!MrUtils.checkOutputMR()){
        	endTime();
        	System.exit(0);
        }
        
        Main.countDir++;
        m.job2();
        if(!MrUtils.checkOutputMR()){
        	endTime();
        	System.exit(0);
        }
        
        int l = 0;
        while(MrUtils.checkOutputMR() && k != -1){
            Main.countDir++;
            k++;
            System.out.println("Map 3 com k = "+k);
            System.out.println("LOOP "+ ++l);
            m.job3();
            k = MrUtils.getK();
        }

        endTime();
    }
}
