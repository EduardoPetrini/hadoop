/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package app;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;
import mapred.Map1;
import mapred.Map2;
import mapred.Map2v2;
import mapred.Map3v2;
import mapred.Reduce1;
import mapred.Reduce2;
import mapred.Reduce2v2;
import mapred.Reduce3v2;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author eduardo
 */
public class Main {

    private Log log = LogFactory.getLog(Main.class);
    public static int countDir;
    private int timeTotal;
    int support;
    
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
            job = new Job(c);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        job.setJobName("Fase 1");
        
        job.setJarByClass(Main.class);
        
        job.setMapperClass(Map1.class);
//        job.setCombinerClass(Combiner1.class);
        job.setReducerClass(Reduce1.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        
        /* Defines additional single text based output 'text' for the job
        "text" é o tipo de arquivo
        3º e 4º argumento refere-se a chave e valor respectivamente.
        */
//        MultipleOutputs.addNamedOutput(job, "invertido", TextOutputFormat.class, IntWritable.class, Text.class);
        
        job.getConfiguration().set("count", String.valueOf(Main.countDir));
        job.getConfiguration().set("support", String.valueOf(support));
        
        try {
            FileInputFormat.setInputPaths(job, new Path("input"));
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        System.out.println("Job 1 - CountDir: "+Main.countDir);
        
        FileOutputFormat.setOutputPath(job, new Path("output"+Main.countDir));
        
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
            job = new Job(c);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        job.setJobName("Fase 2");
        
        job.setJarByClass(Main.class);
        
        job.setMapperClass(Map2v2.class);
//        job.setCombinerClass(Reduce2v2.class);
        job.setReducerClass(Reduce2v2.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setNumReduceTasks(2);
        /* Defines additional single text based output 'text' for the job
        "text" é o tipo de arquivo
        3º e 4º argumento refere-se a chave e valor respectivamente.
        */
        
//        MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, LongWritable.class, Text.class);
        job.getConfiguration().set("count", String.valueOf(Main.countDir));
        job.getConfiguration().set("support", String.valueOf(support));
        
        System.out.println("Job 2 - CountDir: "+Main.countDir);
        
        try {
           job.addCacheFile(new URI("/user/hadoop/invert/invertido"+(Main.countDir-1)));
        } catch (URISyntaxException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        try {
            Path p = new Path("output"+(Main.countDir-1));
//            
//            if(increseMapTask(p, c)){
//                System.out.println("Aumentando o número de task map que está definido como: "+c.get("mapreduce.tasktracker.map.tasks.maximum"));
//                c.set("mapreduce.tasktracker.map.tasks.maximum","2");
//            }
            
            FileInputFormat.setInputPaths(job, p);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        FileOutputFormat.setOutputPath(job, new Path("output"+Main.countDir));
        try {
            long ini = System.currentTimeMillis();
            int st = (job.waitForCompletion(true) ? 0 : 1);
            long fim = System.currentTimeMillis();
            
            long t = fim - ini;
            System.out.println("Tempo da fase 2: "+((double)t/1000));
            
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
            job = new Job(c);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        job.setJobName("Fase 3");
        
        job.setJarByClass(Main.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(Map3v2.class);
//        job.setCombinerClass(Reduce3v2.class);d
        job.setReducerClass(Reduce3v2.class);
//        job.setNumReduceTasks(2);
        
        /*Loop*/
        job.getConfiguration().set("count", String.valueOf(Main.countDir));
        job.getConfiguration().set("support", String.valueOf(support));
        
        System.out.println("Job 3 - CountDir: "+Main.countDir);
        try {
            Path p = new Path("output"+(Main.countDir-1));
            
//            if(increseMapTask(p, c)){
//                System.out.println("Aumentando o número de task map que está definido como: "+c.get("mapreduce.tasktracker.map.tasks.maximum"));
//                c.set("mapreduce.tasktracker.map.tasks.maximum","2");
//            }
            
            FileInputFormat.setInputPaths(job, p);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        FileOutputFormat.setOutputPath(job, new Path("output"+(Main.countDir)));
        
        MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, TextOutputFormat.class, Text.class);
        
        try {
           job.addCacheArchive(new URI("/user/hadoop/invert/invertido"+(Main.countDir-1)));
        } catch (URISyntaxException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        try {
            long ini = System.currentTimeMillis();
            int st = (job.waitForCompletion(true) ? 0 : 1);
            long fim = System.currentTimeMillis();
            
            long t = fim - ini;
            System.out.println("Tempo da fase 3: "+((double)t/1000));
            
            timeTotal += t;
            if(st == 1){
                System.exit(st);
            }
            
        } catch (InterruptedException | ClassNotFoundException | IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public boolean increseMapTask(Path file, Configuration c){
        try {
            FileSystem fs = FileSystem.get(c);
            
            FileStatus f = fs.getFileStatus(file);
            
            long blockSize = f.getBlockSize();
            long fileSize = f.getLen();
            
            if((4*blockSize) >= fileSize){
                return false;
            }
            
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return true;
        
    }
    
    /**
     * Deleta o diretório output
     */
    private void delDirs(String d) {
        
        log.info("Excluindo diretórios anteriores...");
        
        Path p = new Path(d);
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            
            if(fs.isDirectory(p)){
                
                if(fs.delete(p, true)){
                    log.info("Excluido diretório -> "+p.getName());
                }else{
                    log.info("Nao foi possivel excluir "+p.getName());
                }
            }else{
                log.info(p.getParent()+""+p.getName()+" Nao eh um diretorio.");
                
                if(fs.isFile(p)){
                    log.info("Eh um arquivo, excluindo...");
                    
                    if(fs.delete(p, true)){
                        log.info("Excluindo arquivo -> "+p.getName());
                    }else{
                        log.info("Nao foi possivel excluir o arquivo -> "+p.getName());
                    }
                }
            }
            
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    private void delOutDirs(String d) {
        
        log.info("Excluindo diretórios anteriores...");
        
        Path p = new Path(d);
        Path aux;
        
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            
            if(fs.isDirectory(p)){
                
                FileStatus[] ff = fs.listStatus(p);
                
                for(FileStatus f: ff){
                    aux = f.getPath();
                    
                    if(aux.getName().startsWith("output")){
                        
                        if(fs.delete(aux, true)){
                            log.info("Excluido diretório -> "+aux.getName());
                            
                        }else{
                            log.info("Nao foi possivel excluir "+aux.getName());
                        }
                    }
                }
                
            }
            
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    public void createTempDir(String d){
     
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            
            if(fs.mkdirs(new Path(d))){
                log.info("Diretorio "+d+" criado com sucesso.");
            }else{
                log.info("Nao foi possivel criar o diretorio: "+d);
            }
            
        } catch (IOException ex) {
            Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
        }
    
    }
    
    public void delContentFiles(String dir){
        Path p = new Path(dir);
        
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            
            if(fs.isDirectory(p)){
                
                log.info(p.getName()+" eh um diretorio!");
                
                FileStatus[] f = fs.listStatus(p);
                log.info("Conteudo do diretorio: ");
                
                for(FileStatus ff: f){
                                        
                    log.info(ff.getPath().getName());
                    
                    if(ff.toString().contains("confIn")) continue;
                    
                    p = ff.getPath();
                    
                    if(fs.isFile(p)){
                        log.info("Deletando: "+p.getName());
                        if(fs.delete(p, true)){
                            
                            log.info("Deletado!");
                        }else{
                            log.info("Falha ao deletar.");
                        }
                    }
                }
            }
            
            
        }catch(IOException ex) {
            Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    public boolean checkOutput(String dir){
        
        Path p = new Path(dir);
        Path aux;
        
        System.out.println("Verificando diretório: "+dir);
        
        try{
            FileSystem fs = FileSystem.get(new Configuration());

                if(fs.isDirectory(p)){

                     FileStatus[] ff = fs.listStatus(p);

                     for(FileStatus f: ff){
                         
                         aux = f.getPath();
                         if(aux.getName().startsWith("part")){
                             System.out.println("Arquivos dentro do dir: "+aux.getName()+" "+f.getLen()/1024+"Kb ou "+f.getLen()+" bytes");
                             
                             if(f.getLen() > 0){
                                return true;
                                 
                             }else{
                                 return false;
                             }
                             
                         }
                     }

                }else{
                    System.out.println("Não é um diretório: "+dir);
                    return false;
                }
        }catch(IOException e){
            System.out.println("ERROR: "+e);
        }
        System.out.println("Não contém part: "+dir);
        return false;
    }
    
    public static void main(String[] args) throws IOException {
        Main m = new Main();
//        System.out.println(m.checkOutput("output1"));
        m.delOutDirs("/user/hadoop/");
        m.delContentFiles("invert");

        if(args.length > 0){
            m.support = Integer.parseInt(args[0]);
            System.out.println("Valor de suporte: "+m.support);
        }else{
            System.out.println("Erro com o argumento!");
            System.exit(-1);
        }
        
        /*Remover os arquivos invertidos anteriores*/
        
        Main.countDir++;
        m.job1();
        m.checkOutput("output"+Main.countDir);
        
        Main.countDir++;
        m.job2();
        m.checkOutput("output"+Main.countDir);

        int l = 0;
        while(m.checkOutput("output"+Main.countDir)){
            System.out.println("LOOP "+l++);
            
            Main.countDir++;
            m.job3();
        }
        
//        m.delOutDirs("/user/hadoop/");
//        m.delContentFiles("invert");
        
        double seg = ((double)m.timeTotal/1000);
        
        System.out.println("Tempo total: "+m.timeTotal+" mile ou "+seg+" segundos! ou "+seg/60+" minutos");
    }

    private void startJobs() throws IOException, URISyntaxException {
        
        /*Configure JOB 1*/
        JobControl jobControl = new JobControl("Apriori MapReduce");
        
        Configuration c = new Configuration();
        Job job = null;
        try {
            job = new Job(c);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        job.setJobName("TestItemCount");
        job.setJarByClass(Main.class);
        
        job.setMapperClass(Map1.class);
        job.setReducerClass(Reduce1.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        
        
        FileInputFormat.setInputPaths(job, new Path("input"));
        
        FileOutputFormat.setOutputPath(job, new Path("output"));
        
        ControlledJob controle1 = new ControlledJob(c);
        controle1.setJob(job);
            
        jobControl.addJob(controle1);
        
        /*********************************************/
        /*Configure JOB 2*/
        
        Configuration c2 = new Configuration();
        Job job2 = null;
        
        job2 = new Job(c2);
        
        job2.setJobName("TestItemReader");
        
        job2.setJarByClass(Main.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        
        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reduce2.class);


        job2.addCacheFile(new URI("/user/hadoop/output/part-r-00000"));

        FileInputFormat.setInputPaths(job2, new Path("output"));

        FileOutputFormat.setOutputPath(job2, new Path("output2"));
        
        ControlledJob controle2 = new ControlledJob(c);
        controle2.setJob(job2);
        
        controle2.addDependingJob(controle1);
        
        jobControl.addJob(controle2);
        
        /*Start thread*/
        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();
        
        while( !jobControl.allFinished()){
            System.out.println("Jobs in waiting state: "
              + jobControl.getWaitingJobList().size());
            System.out.println("Jobs in ready state: "
              + jobControl.getReadyJobsList().size());
            System.out.println("Jobs in running state: "
              + jobControl.getRunningJobList().size());
            System.out.println("Jobs in success state: "
              + jobControl.getSuccessfulJobList().size());
            System.out.println("Jobs in failed state: "
              + jobControl.getFailedJobList().size());
            //sleep 5 seconds
            try {
              Thread.sleep(5000);
            } catch (Exception e) {  }
        }
        try {
            jobControlThread.join();
        } catch (InterruptedException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
}
