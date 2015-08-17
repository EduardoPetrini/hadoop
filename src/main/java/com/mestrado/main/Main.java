/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.main;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    public static double supportPercentage = 0.52;
    public static String support;
    public static int k = 1;
    public static String user = "/user/eduardo/";
    public static String inputEntry = "input/";
    public static String inputFileName = "";
    public static String clusterUrl = "hdfs://master/";
    public static String fileCached = user+"outputCached/outputMR";
    public static String fileCachedDir = user+"outputCached/";
    public static long totalTransactionCount;
    public static double earlierTime;
    public static ArrayList<String> seqFilesNames;
    public static int NUM_REDUCES = 1;
    /*
    Valor do suporte para 1.000.000
    7500
    10.000
    12.500
    15.000
    17.500
    20.000
    */
    
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
         job.getConfiguration().set("fs.defaultFS", clusterUrl);
        job.setJobName("AprioriDpc Fase 1");
        
        job.setJarByClass(Main.class);
        
        job.setMapperClass(Map1.class);
//        job.setCombinerClass(Combiner1.class);
        job.setReducerClass(Reduce1.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
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
            long ini = System.currentTimeMillis();
            int st = (job.waitForCompletion(true) ? 0 : 1);
            long fim = System.currentTimeMillis();
            
            long t = fim - ini;
            System.out.println("Tempo AprioriDpc Fase 1: "+((double)t/1000));
            
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
         job.getConfiguration().set("fs.defaultFS", clusterUrl);
        job.setJobName("AprioriDpc Fase 2");
        
        job.setJarByClass(Main.class);
        
        job.setMapperClass(Map2.class);
//        job.setCombinerClass(Reduce2.class);
        job.setReducerClass(Reduce2.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
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
            long ini = System.currentTimeMillis();
            int st = (job.waitForCompletion(true) ? 0 : 1);
            long fim = System.currentTimeMillis();
            
            long t = fim - ini;
            earlierTime = ((double)t/1000);
            System.out.println("Tempo AprioriDpc Fase 2: "+earlierTime);
            
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
            long ini = System.currentTimeMillis();
            int st = (job.waitForCompletion(true) ? 0 : 1);
            long fim = System.currentTimeMillis();
            
            long t = fim - ini;
            earlierTime = ((double)t/1000);
            System.out.println("Tempo AprioriDpc Fase 3: "+earlierTime);
            
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
    	sb.append("AprioriDpc - support ").append(supportPercentage).append(", transactions ").append(totalTransactionCount).append(" -- ").append(new Date()).append("\n");
    	sb.append("Arquivo ").append(inputFileName).append("\n\t");
    	sb.append("Tempo total: ").append(timeTotal).append(" mile ou ").append(seg).append(" segundos ou ").append(seg/60).append(" minutos\n------------\n");
        System.out.println("Tempo total: "+timeTotal+" mile ou "+seg+" segundos! ou "+seg/60+" minutos");
        sb.append("Quantidade de itemsets gerados: \n\t");
    	sb.append(CountItemsets.countItemsets());
    	sb.append("\n-----------\n");
        MrUtils.saveTimeLog(sb.toString());
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
