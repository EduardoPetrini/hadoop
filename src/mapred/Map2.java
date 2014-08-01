/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred;

import app.ItemTid;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
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
 *
 * @author eduardo
 */
public class Map2 extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    Log log = LogFactory.getLog(Map2.class);
    
    ItemTid invert;
    ArrayList<IntWritable> keys;
    SequenceFile.Reader reader;
    int minSup = 1;
    
    @Override
    public void setup(Context c) throws IOException{
        
        log.info("Iniciando map 2");
        invert = new ItemTid(); 
        keys = new ArrayList();
        
        reader = new SequenceFile.Reader(c.getConfiguration(), SequenceFile.Reader.file(new Path("/user/hadoop/invert/invertido")));
        
        IntWritable key = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(), c.getConfiguration());
        Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), c.getConfiguration());
        
        String[] values;
        ArrayList<LongWritable> tids;
        
        while (reader.next(key, value)) {
           
            keys.add(new IntWritable(key.get()));
            
            values = value.toString().split(":");
            tids = new ArrayList();
            
            for(String v: values){
                tids.add(new LongWritable(Integer.parseInt(v)));
            }
            
            invert.put(String.valueOf(key.get()), tids);
           
        }
        
        log.info("Imprimindo KEYS: ");

        for(IntWritable k: keys){
            log.info("Arquivo invertido: "+k);
        }
        
        log.info("Dados carregados na memória!");
        
        IOUtils.closeStream(reader);
        
    }
    
    
    /**
     * 
     * @param c 
     *
    @Override
    public void setup(Context c){
        log.info("Iniciando o map 2...");
        
        invert = new ItemTid();
        int count = 0;
        
        BufferedReader in = null;
        URI[] uri = null;
                
        try {
            uri = c.getCacheFiles();
        } catch (IOException ex) {
            Logger.getLogger(Map2.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        /*Leitura do arquivo
        try {
            log.info("Lendo arquivo invertido gerado no MR anterior...");
            
            if(uri != null){
                
                for(URI u: uri){
                    log.info("Arquivos no URI: "+u.toString());
                    
                    if(u.toString().endsWith("invertido-r-00000")){
                        
                        in = new BufferedReader(new FileReader(u.toString()));//Arquivo de saída do reduce1
                    }
                }
                
            }else{
                log.info("URI é NULO.................");
                in = null;
                System.exit(1);
            }
            
            
            String buf;
            
            String[] it;
            ArrayList<LongWritable> trans;
            
            if(in != null){
                log.info("BUFFER de entrada NÃO é nulo!!! loop "+(count+1));
                while((buf = in.readLine()) != null){
                    
                    it = buf.split("\\s+");
                    
                    trans = new ArrayList(Arrays.asList(it[1].split(":")));
                    
                    invert.put(new IntWritable(Integer.parseInt(it[0])), trans);
                    
                    count++;
                }

                log.info(count+" linhas lidas do arquivo!!!");

                if(count == 0){
                    log.info("Problemas com leitura do arquivo!");
                }
            }else{
                log.info("Buffer de entrada é nulo (NULO)!");
                System.exit(1);
            }
            
            
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Map2.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Map2.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        keys = new ArrayList(Arrays.asList(invert.keySet().toArray()));
        
        
//        try {
//            
//            /* Primeiro argumento é a chave
//             * segundo argumento é o valor
//             * terceiro argumento é o nome do arquivo
//             */
//            mo.write(new LongWritable(99), new Text("valor_da_chave"), "texxt");
//        } catch (IOException | InterruptedException ex) {
//            Logger.getLogger(Map2.class.getName()).log(Level.SEVERE, null, ex);
//        }
        
    //}*/
    
    /**
     * 
     * @param item
     * @param combinations
     * @param k
     * @param pos
     * @param context 
     */
    private void generateKItemsets(String item, int k, int pos, ArrayList<LongWritable> v, Context context, int lim) {
       
        /* Não inventar soluções, busca algo funcional já definido */
        
        StringBuilder sb;
        int sup = 0;
        
        ArrayList<LongWritable> aux;
        
        for (int i = pos; i < keys.size(); i++) {
            sb = new StringBuilder();

            sb.append(item).append(" ").append(keys.get(i));
            
            /*Verifica se o tamanho da combinação é ímpar, caso seja efetuar a poda*/
            if(k%2 == 0){
                
                if((aux = poda(sb.toString(), v, k)) == null){
                   continue; 
                }else{
                    v = aux;
                }
            }
            
            try {
                
                context.write(new Text(sb.toString()), new IntWritable(sup));
            } catch (IOException | InterruptedException ex) {
                Logger.getLogger(Map2.class.getName()).log(Level.SEVERE, null, ex);
            }

//            if(lim <= 2){
//                generateKItemsets(sb.toString(), k+1, i+1, v, context, lim+1);
//            }
        }
    }
    
    
    private void generateKItemsets2(String item, ArrayList<String> comb, int k, int pos, ArrayList<LongWritable> v, Context context, int lim) {
       
        StringBuilder sb;
        
        ArrayList<LongWritable> aux;
        
        /*Gera-se os kitemsets - aramazena em um vetor temporário*/
        for (int i = pos; i < keys.size(); i++) {
            sb = new StringBuilder();

            sb.append(item).append(" ").append(keys.get(i));
            
            if(k%2 != 0){
                
                if((aux = poda(sb.toString(), v, k)) == null){
                   continue; 
                }else{
                    v = aux;
                }
            }
            
            comb.add(sb.toString());
        }
        
        for(int i = 0; i < comb.size(); i++){
            
            generateKItemsets2(comb.get(i), comb, k+1, pos+i, v, context, lim);
            
        }
        
    }
    
    /**
     * 
     * @param item
     * @param pos 
     * @param k 
     * @param itenSup 
     */
    public void genericKitemset(String[] item, int pos, int k, ArrayList<LongWritable> itenSup, Context context) throws IOException, InterruptedException{
        
        String[] novo;
        
        ArrayList<LongWritable> novoItenSup;
        ArrayList<LongWritable> aux;
        
        Text key = new Text();
        IntWritable value = new IntWritable();
        
        k++;
        
        while(pos < keys.size()){
            
            novo = new String[2];
            novo[0] = item[0] + " " + item[1];
            novo[1] = String.valueOf(keys.get(pos));
            
            novoItenSup = invert.get(keys.get(pos));
            log.info("Combinations: "+novo[0]+" "+novo[1]+" suport: "+ItemTid.intersection2(itenSup, novoItenSup).size());
            
            if((aux = ItemTid.calcSimi(itenSup, novoItenSup)).size() >= 1 ){  
                itenSup = aux;
                
                //envia para o reduce
                key.set(novo[0]+" "+novo[1]);
                value.set(itenSup.size());
                context.write(key, value);    
                
                //gerar os k+1 itensets
                genericKitemset(novo, pos+1, k, itenSup, context);
                
            }
            pos++;
            
//            if(k % 2 != 0){
//                //poda
//                
//            }
            
            
        }
    }
    
    /**
     * 
     * @param key
     * @param value
     * @param context 
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        
        
        String[] sp = value.toString().split("\\s");

        int begin = keys.indexOf(new IntWritable(Integer.parseInt(sp[0]))) + 1;

        //item, vetor de combinações, k, posicao do item na hash e o context

        log.info("Item: "+sp[0]);
        String[] sb;
        
        ArrayList<LongWritable> itenSup;
        
        Text keyOut = new Text();
        IntWritable valueOut = new IntWritable();
        
        for(int i = begin; i < keys.size(); i++){
            sb = new String[2];
            sb[0] = sp[0];
            
            sb[1] = String.valueOf(keys.get(i));
            
            log.info("2k: "+sb[0]+" "+sb[1]);
            if((itenSup = ItemTid.calcSimi(invert.get(new IntWritable(Integer.parseInt(sb[0]))), invert.get(new IntWritable(Integer.parseInt(sb[1]))))).size() >= 1){
                keyOut.set(sb[0]+" "+sb[1]);
                valueOut.set(itenSup.size());
                
                context.write(keyOut, valueOut);
                
                genericKitemset(sb, i+1, 2, itenSup, context);
                
            }
        }
    }
    
//    @Override
//    public void map(LongWritable key, Text value, Context context){
//        
//       String[] sp = value.toString().split("\\s");
//       
//       IntWritable item = new IntWritable(Integer.parseInt(sp[0]));
//       int pos = keys.indexOf(item);
//       
//       //item, vetor de combinações, k, posicao do item na hash e o context
//       
//       log.info("Item: "+sp[0]);
//       generateKItemsets(sp[0], 1, pos+1, invert.get(item), context, 1);
//        
//    }
//    
    

    private ArrayList<LongWritable> poda(String comb, ArrayList<LongWritable> vs, int k) {
        
        String[] it = comb.split(" ");
        
        int i = 1;
        Set s;
        
        if((vs = ItemTid.calcSimi2(vs, invert.get(new IntWritable(Integer.parseInt(it[i]))))).size() < minSup){
            return null;
        }
            
        return vs;
    }

}
