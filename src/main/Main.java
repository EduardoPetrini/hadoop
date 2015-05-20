/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;

import mapred.map.Map1;
import mapred.map.Map2;
import mapred.map.Map3;
import mapred.reduce.Reduce1;
import mapred.reduce.Reduce2;
import mapred.reduce.Reduce3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import utils.MrUtils;

/**
 *
 * @author eduardo
 */
public class Main {

    public static int countDir;
    private int timeTotal;
    public static double supportPercentage = 0.005;
    public static String support;
    int k = 1;
    public static String user = "/user/eduardo/";
    public static String inputEntry = "input/T10I4D10N1000K.100.ok";
    public static String clusterUrl = "hdfs://master/";
    public static 	String fileCached = user+"outputCached/outputMR";
    public static long totalTransactionCount;
    double earlierTime;
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
        job.setJobName("Fase 1");
        
        job.setJarByClass(Main.class);
        
        job.setMapperClass(Map1.class);
//        job.setCombinerClass(Combiner1.class);
        job.setReducerClass(Reduce1.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.getConfiguration().set("count", String.valueOf(Main.countDir));
        job.getConfiguration().set("support", String.valueOf(support));
        job.getConfiguration().set("fileCached", fileCached+(Main.countDir));
        
        try {
            FileInputFormat.setInputPaths(job, new Path(user+"input"));
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        System.out.println("Job 1 - CountDir: "+Main.countDir);
        
        FileOutputFormat.setOutputPath(job, new Path(user+"output"+Main.countDir));
        
        try {
            long ini = System.currentTimeMillis();
            int st = (job.waitForCompletion(true) ? 0 : 1);
            long fim = System.currentTimeMillis();
            
            long t = fim - ini;
            System.out.println("Tempo da fase 1: "+((double)t/1000));
            
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
        job.setJobName("Fase 2");
        
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
        job.getConfiguration().set("fileCachedRead", fileCached+(Main.countDir-1));
        job.getConfiguration().set("fileCachedWrited", fileCached+(Main.countDir));
          
        System.out.println("Job 2 - CountDir: "+Main.countDir);
        
        try {
           job.addCacheFile(new URI(fileCached+(Main.countDir-1)));
        } catch (URISyntaxException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
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
            System.out.println("Tempo da fase 2: "+earlierTime);
            
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
        job.setJobName("Fase 3");
        
        job.setJarByClass(Main.class);
        
        job.setMapperClass(Map3.class);
//        job.setCombinerClass(Reduce2.class);
        job.setReducerClass(Reduce3.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.getConfiguration().set("count", String.valueOf(Main.countDir));
        job.getConfiguration().set("support", String.valueOf(support));
        job.getConfiguration().set("k", String.valueOf(k));
        job.getConfiguration().set("fileCachedRead", fileCached+(Main.countDir-1));
        job.getConfiguration().set("fileCachedWrited", fileCached+(Main.countDir));
        job.getConfiguration().set("earlierTime", String.valueOf(earlierTime));
        System.out.println("Job 3 - CountDir: "+Main.countDir);
        
        try {
           job.addCacheFile(new URI(fileCached+(Main.countDir-1)));
        } catch (URISyntaxException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
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
            System.out.println("Tempo da fase 3: "+earlierTime);
            
            timeTotal += t;
            if(st == 1){
                System.exit(st);
            }
            
        } catch (InterruptedException | ClassNotFoundException | IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void main(String[] args) throws IOException {
        Main m = new Main();
        MrUtils.delOutDirs(user);
        MrUtils.initialConfig();
        
        Main.countDir++;
        MrUtils.printConfigs(m);
        
        m.job1();
        MrUtils.checkOutputMR();
        
        Main.countDir++;
        m.job2();
        MrUtils.checkOutputMR();
        
        int l = 0;
        while(MrUtils.checkOutputMR() && m.k != -1){
        	System.out.println("Map 3 com k = "+m.k);
            System.out.println("LOOP "+ ++l);
            Main.countDir++;
            m.k++;
            m.job3();
            m.k = MrUtils.getK(m.k);
        }

        /*Remover os arquivos invertidos anteriores*/
//        m.delOutDirs(user);
//        m.delContentFiles("invert");
        
        double seg = ((double)m.timeTotal/1000);
        
        System.out.println("Tempo total: "+m.timeTotal+" mile ou "+seg+" segundos! ou "+seg/60+" minutos");
    }
   
}
