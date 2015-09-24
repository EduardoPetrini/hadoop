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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import main.java.com.mestrado.hadoop.inputformat.WholeInputFormat;
import main.java.com.mestrado.mapred.combiner.CombinerGen;
import main.java.com.mestrado.mapred.map.GenMap;
import main.java.com.mestrado.mapred.map.Map1;
import main.java.com.mestrado.mapred.map.Map2;
import main.java.com.mestrado.mapred.reduce.GenReduce;
import main.java.com.mestrado.mapred.reduce.Reduce1;
import main.java.com.mestrado.mapred.reduce.Reduce2;
import main.java.com.mestrado.utils.AprioriUtils;
import main.java.com.mestrado.utils.CountItemsets;
import main.java.com.mestrado.utils.MrUtils;

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
    public static String outputCandidates = user+"outputCandidates/C";
    public static String inputCandidates = user+"inputCandidates/C";
    public static String inputCandidatesDir = user+"inputCandidates";
    public static String inputFileToGen = user+"inputToGen/input";
    public static long totalTransactionCount;
    public static ArrayList<String> candFilesNames;
    public static int NUM_REDUCES = 3;
    public static String NUM_BLOCK = "2b";
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
        job.setJobName("AprioriCpa Fase 1");
        
        job.setJarByClass(Main.class);
        
        job.setMapperClass(Map1.class);
//        job.setCombinerClass(Reduce1.class);
        job.setReducerClass(Reduce1.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(WholeInputFormat.class);
        
        job.getConfiguration().set("count", String.valueOf(Main.countDir));
        job.getConfiguration().set("support", String.valueOf(support));
        
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
            System.out.println("Tempo AprioriCpa Fase 1: "+((double)t/1000));
            
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
    public void jobCount(){
        
        Configuration c = new Configuration();
        
        Job job = null;
        try {
            job = Job.getInstance(c);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
         job.getConfiguration().set("fs.defaultFS", clusterUrl);
        job.setJobName("AprioriCpa Contagem");
        
        job.setJarByClass(Main.class);
        
        job.setMapperClass(Map2.class);
//        job.setCombinerClass(Reduce2.class);
        job.setReducerClass(Reduce2.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(WholeInputFormat.class);
        
        job.getConfiguration().set("count", String.valueOf(Main.countDir));
        job.getConfiguration().set("support", String.valueOf(support));
        job.getConfiguration().set("k", String.valueOf(k));
        job.getConfiguration().set("candsize", String.valueOf(candFilesNames.size()));
        for(int i =0; i < candFilesNames.size(); i++){
        	job.getConfiguration().set("inputCandidates"+i, candFilesNames.get(i));//Contém Ck
        	try {
        		job.addCacheFile(new URI(candFilesNames.get(i)));
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
            System.out.println("Tempo AprioriCpa Fase de contagem (k = "+k+"): "+((double)t/1000));
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
    public void jobGen(){
        
        Configuration c = new Configuration();
        
        Job job = null;
        try {
            job = Job.getInstance(c);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
         job.getConfiguration().set("fs.defaultFS", clusterUrl);
        job.setJobName("AprioriCpa Geracao");
        
        job.setJarByClass(Main.class);
        
        job.setMapperClass(GenMap.class);
        job.setCombinerClass(CombinerGen.class);
        job.setReducerClass(GenReduce.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(WholeInputFormat.class);
        
        job.getConfiguration().set("inputCandidates", inputCandidates+Main.countDir);
        job.getConfiguration().set("inputFileToGen", inputFileToGen);
        try{
        	job.addCacheFile(new URI(inputFileToGen));
        }catch(URISyntaxException e){
        	e.printStackTrace();
        }
        System.out.println("AprioriCpa geração de candidatos - CountDir: "+Main.countDir);
        
        job.setNumReduceTasks(NUM_REDUCES);
        
        try {
        	FileInputFormat.setInputPaths(job, new Path(inputFileToGen));//Entra Lk, do job count
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        FileOutputFormat.setOutputPath(job, new Path(user+"candidatosTxt"+Main.countDir));//Sai Ck+1
        try {
//            long ini = System.currentTimeMillis();
            int st = (job.waitForCompletion(true) ? 0 : 1);
//            long fim = System.currentTimeMillis();
            long ini = job.getStartTime();
            long fim = job.getFinishTime();
            long t = fim - ini;
            System.out.println("Tempo AprioriCpa Fase de geração (k = "+k+"): "+ ((double)t/1000));
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
    
    public static boolean checkOutputSequence(){
    	if(!MrUtils.checkOutputMR()){
        	System.out.println("Arquivo gerado na fase "+countDir+" é vazio!!!\n");
//    		endTime();
//    		System.exit(0);
    		return false;
        }
    	return true;
    }
    
    /**
     * 
     * @return
     */
    public static boolean checkCountOutput(){
    	if(!MrUtils.checkOutput(user+"output"+Main.countDir)){
        	System.out.println("Arquivo gerado na fase "+countDir+" é vazio!!!\n");
//    		endTime();
//    		System.exit(0);
    		return false;
        }
    	return true;
    }
    
    public static boolean checkInputSequence(){
    	if(!MrUtils.checkInputMR()){
        	System.out.println("Arquivo gerado na fase "+countDir+" é vazio!!!\n");
    		endTime();
    		return false;
        }
    	return true;
    }
    
    public static void copyToInputGen(){
    	MrUtils.copyToInputGen(user+"output"+(Main.countDir-1));
    }
    
    public static void main(String[] args) throws IOException {
        Main m = new Main();
        MrUtils.delOutDirs(user);
        MrUtils.initialConfig(args);
        
        Main.countDir++;//1
        MrUtils.printConfigs(m);
        //Main.k == 1
        m.job1();
        
        //check output dir
        if(!MrUtils.checkOutput(user+"output"+Main.countDir)){
        	endTime();
        	System.exit(0);
        }
        long time2k = 0;
        if((time2k = AprioriUtils.generate2ItemsetCandidates()) == -1){
        	endTime();
        	System.exit(0);
        }
        System.out.println("Tempo da fase geração de k = 2: "+time2k);
        timeTotal += time2k;
        
        Main.k++; //Main.k == 2;
        Main.countDir++;//2
        do{
	        m.jobCount();//Contar a corrência de Ck na base. Le Ck e salva Lk
	        if(!checkCountOutput()){
	        	break;
	        }
	        //copia Lk do output do jobCount para o input do jobGen
	        Main.k++; //Main.k == 3;
	        Main.countDir++;//3
	        copyToInputGen();
	        //gera lk+1
	        m.jobGen();
	        
        }while(checkOutputSequence());
       
        endTime();
    }
}
