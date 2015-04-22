/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package app;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author hadoop
 */
public class Teste {
    
    ArrayList<String> itens;
    int count = 0;
    
    
    public void map() throws Exception{
        itens = new ArrayList();
        
//        itens.add("a");
//        itens.add("b");
//        itens.add("c");
//        itens.add("d");
//        itens.add("e");
//        itens.add("f");
//        itens.add("g");
//        itens.add("h");
//        itens.add("i");
//        itens.add("j");
//        for (int i = 0; i < 8; i++) {
//            itens.add(""+(i+1));
//        }
        
//        for(String item : itens){
//             generateKItemsets(item, 1, itens.indexOf(item)+1);
//        }
//        System.out.println("\n\ngenerate 2\n\n");
        
//        for(String item : itens){
//             geraManager2(item);
//        }
       
        
        Evaluator.evaluator(new Callable() {

                public Integer call() {
                        hash1();
                        return 0;
                }
        }, new Callable() {

                public Integer call() {
                        hash2();
                        return 0;
                }
        }, 30, 20);//vezes
        
    }
    
    public void hash1(){
        HashMap<String,String> h = new HashMap();
        ArrayList<String> a = new ArrayList();
        String k;
        String v;
        int size = 1000;
        for (int i = 0; i < size; i++) {
            k = i+"";
            v = size-i+"";
            h.put(k, v);
            a.add(k);
        }
        
        for (int i = 0; i < size; i++) {
            a.indexOf(i+"");
            h.get(size-i+"");
        }

    }
    public void hash2(){
        StructTeste h = new StructTeste();
        int size = 1000;
        String k;
        String v;
        for (int i = 0; i < size; i++) {
            k = i+"";
            v = size-i+"";
            h.put(k, v);
        }
        
        for (int i = 0; i < size; i++) {
            h.indexOF(i+"");
            h.get(size-1+"");
        }
        
    }
    
    private void generateKItemsets(String item, int k, int pos) {
       
        /* Não inventar soluções, busca algo funcional já definido */
        
        StringBuilder sb;
        
        for (int i = pos; i < itens.size(); i++) {
            sb = new StringBuilder();

            sb.append(item).append(" ").append(itens.get(i));
//            count++;
            
//            System.out.println(sb.toString());
//            if(count%100 == 0) System.out.println(count);
            generateKItemsets(sb.toString(), k+1, i+1);
            
        }        
//         System.out.println(count);
        
    }
    
    
    public void geraManager3(String item){
        
        int begin = itens.indexOf(item)+1;
        String[] sb;
        
        ArrayList<String[]> kitens = new ArrayList();
        
        for(int i = begin; i < itens.size(); i++){
            sb = new String[2];
            sb[0] = item;
            
            sb[1] = itens.get(i);
            
            kitens.add(sb);
            
        }
        
        int k = 2;
        
//        for(String[] as: kitens){
//            System.out.println(as[0]+","+as[1]);
//        }
        genericKitemset3(kitens);
        
    }
     
    /**
     * 
     * @param k2itemset
     */
    public void genericKitemset3(ArrayList<String[]> k2itemset){
        
        ArrayList<String[]> kitemset = new ArrayList();
//        ArrayList<LongWritable> v;
        
        for (int i = 0; i < k2itemset.size()-1; i++) {
            
            /*Obter o suporte de cada kitemsets*/
            
            int j = i+1;
            StringBuilder sb;
            sb = new StringBuilder();

            sb.append(k2itemset.get(i)[0]);
            sb.append(" ");
            sb.append(k2itemset.get(i)[1]);
            String[] novo;
            
            while(k2itemset.get(i)[0].equals(k2itemset.get(j)[0])){
                
                //Efetuar a poda
                /* Dado o vetor de interseção dos k itens, obter o suporte do novo item a ser adicionado
                   e efetuar a interseção dos do k itens com o novo item.
                   Se podar, continua o While, se não adiciona a nova combinação (k+1) no vetor auxiliar.
                */
                novo = new String[2];
                novo[0] = sb.toString();
                novo[1] = k2itemset.get(j)[1];
                // Manda pro reduce
//                System.out.println(novo[0]+","+novo[1]);
                kitemset.add(novo);
                if(j == k2itemset.size()-1){
                    break;
                }
                j++;
            }
        }
        
        //Chamada recursiva se o novo vetor não for vazio
        if(!kitemset.isEmpty()){
            genericKitemset3(kitemset);
        }
        
    }
    
    public void geraManager2(String item){
        
        int begin = itens.indexOf(item)+1;
        String[] sb;
        
        for(int i = begin; i < itens.size(); i++){
            sb = new String[2];
            sb[0] = item;
            
            sb[1] = itens.get(i);
            System.out.println(sb[0]+" "+sb[1]);
            
            genericKitemset2(sb, i+1);
            
        }
        
        int k = 2;
        
//        for(String[] as: kitens){
//            System.out.println(as[0]+","+as[1]);
//        }
        
    }
     
    /**
     * 
     * @param k2itemset
     */
    public void genericKitemset2(String[] k2itemset, int pos){
        
        String[] novo;
        
        while(pos < itens.size()){
            novo = new String[2];
            novo[0] = k2itemset[0] + " " + k2itemset[1];
            novo[1] = itens.get(pos);
            System.out.println(novo[0]+" "+novo[1]);
            genericKitemset2(novo, pos+1);
            pos++;
            
        }
        
    }
     
    
    
    public void analiseBase2(String fileName){
        
        File file = new File(fileName);
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Teste.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        String[] tokens;
        int countT = 0;
        double lNum = 0;
        HashMap<Integer, Byte> hash = new HashMap();
        String l;
        
        try {
            while((l = br.readLine()) != null){
                lNum++;
                
                tokens = l.split(" ");
                countT += tokens.length-1;
                

                for (int i = 1; i < tokens.length; i++) {
                    hash.put(Integer.parseInt(tokens[i]), Byte.MIN_VALUE);
                }

                if((lNum % 100000) == 0 ){
                    System.out.println("\nMédia parcial: "+(countT/lNum));
                    System.out.println("Quantidade de itens parcial: "+hash.keySet().size());
                }
            }
            System.out.println("\n----------\nLinhas lidas: "+lNum);
            System.out.println("Média de itens por transação: "+(double)(countT/lNum));
            System.out.println("Quantidade de itens: "+hash.keySet().size());
            
            System.out.println("Itens:");
            ArrayList<Integer> a = new ArrayList(Arrays.asList(hash.keySet().toArray()));
            
//            Collections.sort(a);
//            for(Integer k: a){
//                System.out.println(k);
//            }
        } catch (IOException ex) {
            Logger.getLogger(Teste.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    public void processIbmData(String fileName){
        
        File file = new File(fileName);
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Teste.class.getName()).log(Level.SEVERE, null, ex);
        }
        String line;
        String out = "/home/eduardo/Documentos/T10I4D100KN1000K";
        System.out.println("Salvando dados no disco...");
        int countAux  = 1;
        try {
            while ((line = br.readLine()) != null) {
//                System.out.println(line);
                line = line.replaceAll("[0-9]+ [0-9]+ (.*)", "$1");
//                line = line.replaceAll("[0-9]+ [0-9]+ [0-9]+ (.*)", "$1");
//                System.out.println(line);
                line = sort(line);
//                System.out.println(line);
//                System.out.println("");
                save(countAux+" "+line,out);
                countAux++;
                
                if(countAux%100000 == 0){
                    System.out.println("Salvando linha "+countAux);
                }
            }
        } catch (IOException ex) {
            Logger.getLogger(Teste.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            br.close();
        } catch (IOException ex) {
            Logger.getLogger(Teste.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    
    public void analiseBase(String file){
        
        String[] tran = Teste.loadFile(file);
        
        ArrayList<Integer> vet = new ArrayList();
        ArrayList<Integer> vet1 = new ArrayList();
        StringTokenizer line;
        int i;
        
        int count = 0;
        
        for(String s: tran){
            line = new StringTokenizer(s);
            count ++;
            if(count %1000 == 0) System.out.println(count);
            if((i = vet.indexOf(line.countTokens()-1)) != -1){
                
                vet1.add(i, vet1.get(i)+1);
                
            }else{
                vet.add(line.countTokens());
                vet1.add(1);
            }
            
        }
        int media = 0;
        for (int j = 0; j < vet.size(); j++) {
            System.out.print(vet.get(j)+" "+vet1.get(j));
            media += vet.get(j);
            System.out.println("");
        }
        
        System.out.println("Soma: "+media);
        System.out.println("Media: "+media/vet.size());
        
    }
    
    public void addLineNumber(String original){
        
        File file = new File(original);
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Teste.class.getName()).log(Level.SEVERE, null, ex);
        }
        String line;
        String out = "/home/eduardo/Documents/2milhao2";
        System.out.println("Salvando dados no disco...");
        int countAux  = 1;
        try {
            while ((line = br.readLine()) != null) {
//                System.out.println(line);
                line = line.replaceAll(" +[0-9]+\\.[0-9]+ +[0-9]+\\.[0-9]+ +", "");
                line = sort(line);
//                System.out.println(line);
//                System.exit(0);
                save(countAux+" "+line,out);
                countAux++;
                
                if(countAux%100000 == 0){
                    System.out.println("Salvando linha "+countAux);
                }
            }
        } catch (IOException ex) {
            Logger.getLogger(Teste.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            br.close();
        } catch (IOException ex) {
            Logger.getLogger(Teste.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
    }
    
    public void save(String data, String fileOut){
        File file = new File(fileOut);
        FileOutputStream fos = null;
        
        try{
            fos = new FileOutputStream(file, true);
            fos.write(data.getBytes());
            fos.write("\n".getBytes());
            fos.close();
            
        }catch(IOException e){
            System.out.println("Erro ao salvar o arquivo!");
            Logger.getLogger(Teste.class.getName()).log(Level.SEVERE, null, e);
        }finally{
            if(fos != null){
                try {
                    fos.close();
                } catch (IOException ex) {
                    Logger.getLogger(Teste.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }
    
    public void saveDataWithLine(String[] data, String fileOut){
        
        File file = new File(fileOut);
        FileOutputStream fos = null;
        
        try{
            fos = new FileOutputStream(file);
            for(int i = 0; i < data.length; i++){
                fos.write(((i+1)+" "+data[i]+"\n").getBytes());
                
                if(i%100000 == 0){
                    System.out.println("Salvando linha "+i+1);
                }
            }
            fos.close();
        }catch(IOException e){
            System.out.println("Erro ao salvar o arquivo!");
            Logger.getLogger(Teste.class.getName()).log(Level.SEVERE, null, e);
        }finally{
            if(fos != null){
                try {
                    fos.close();
                } catch (IOException ex) {
                    Logger.getLogger(Teste.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        
        

    }
    
    
    
     public static String[] loadFile(String fileWay){
        FileInputStream fis = null; 
        
        try{
            File file = new File(fileWay);
            fis = new FileInputStream(file);

            byte[] b = new byte[(int)file.length()];

            fis.read(b);
            return new String(b).split("\n");
        }catch(Throwable e){            
            Logger.getLogger(Teste.class.getName()).log(Level.SEVERE, "Arquivo não encontrado!"+e,new IOException("ERROR"));
        }finally{
                    
            if(fis != null)try {
                fis.close();
            } catch (IOException ex) {
                Logger.getLogger(Teste.class.getName()).log(Level.SEVERE, "Caca ao ler o arquivo! "+ex, ex);
            }
        }
        System.out.println(" point sfd ");
        return null;
    }
    


public void loadFileBinaryFormat(String path) throws IOException {
        String thisLine;
        // BufferedReader myInput = null;
        DataInputStream myInput = null;
        try {
            FileInputStream fin = new FileInputStream(new File(path));
            myInput = new DataInputStream(fin);
            
            ArrayList<ArrayList<String>> sequences = new ArrayList();
            ArrayList<String> sequence = new ArrayList();
            String itemset = new String();
            while (myInput.available() != 0) {
                int value = INT_little_endian_TO_big_endian(myInput.readInt());
                
               String s = new String();
               
               
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

	// This function was written by Anghel Leonard:
	int INT_little_endian_TO_big_endian(int i) {
		return ((i & 0xff) << 24) + ((i & 0xff00) << 8) + ((i & 0xff0000) >> 8)
				+ ((i >> 24) & 0xff);
	}

    private String sort(String line) {
        String[] l = line.split(" ");
        ArrayList<Integer> a = new ArrayList();
        
        for(String s: l){
            if(s.matches("[0-9]+")){
                
                a.add(Integer.parseInt(s));
            }
        }
        Collections.sort(a);
        
        StringBuilder sb = new StringBuilder();
        
        for(Integer i: a){
            sb.append(i).append(" ");
        }
        
        return sb.toString().trim();
    }
    
    /**
     * 
     * @param itemset
     * @param transactions
     * @return
     */
    public int countByMatches(String itemset, ArrayList<String> transactions){
    	int count = 0;
    	String[] itemsetSplit = itemset.split(" ");
    	String pattern = "";

    	for(String item: itemsetSplit){
    		pattern += ".*"+item;
    	}

    	pattern += ".*";

    	for(String t: transactions){
    		if(t.matches(pattern)){
    			count ++;
    		}
    	}
    	return count;
    }
    
    /**
     * 
     * @param itemset
     * @param transactions
     * @return
     */
    public int countByEquals(String itemset, ArrayList<String> transactions){
    	int count = 0;
    	
    	String[] itemsetSplit = itemset.split(" ");
    			
		boolean itemCombina = false;
		
		for(String t: transactions){
			String[] transSplit = t.split(" ");
			int index = 0;
			
			for_item:
			for(int j = 0; j < itemsetSplit.length; j++){
				itemCombina = false;
				for(int x = index; x < transSplit.length; x++){
					if(itemsetSplit[j].equals(transSplit[x])){
						index = x+1;
						itemCombina = true;
						continue for_item;
					}
				}
				if(!itemCombina){
					break for_item;
				}
			}
			if(itemCombina){
				count++;
			}
		}
    	
    	return count;
    }
    
    /**
     * 
     * @param itemset
     * @param transactions
     * @return
     */
    public int countByEquals2(String itemset, ArrayList<String> transactions){
    	int count = 0;
    	
    	String[] itemsetSplit = itemset.split(" ");
    			
		boolean itemCombina;
		String[] tSplit;
		int j,i;
		
		for_t:
		for(String t: transactions){
			itemCombina = true;
			tSplit = t.split(" ");
			j = 0;i = 0;
			while(itemCombina){
				if(itemsetSplit[i].equals(tSplit[j])){
					if(++i == itemsetSplit.length || 
							++j == tSplit.length){
						count++;
						continue for_t;
					}
					
				}else{
					if(++j == tSplit.length){
						continue for_t;
					}
				}
			}
		}
		
    	
    	return count;
    }
    
    public static void main(String[] args) throws Exception {
    	HashMap<String, Integer> itemSup = new HashMap<String, Integer>(4);
    	addToHashItemSup(itemSup,"a");
    	addToHashItemSup(itemSup,"b");
    	addToHashItemSup(itemSup,"a");
    	addToHashItemSup(itemSup,"b");
    	addToHashItemSup(itemSup,"a");
    	addToHashItemSup(itemSup,"d");
    	addToHashItemSup(itemSup,"e");
    	addToHashItemSup(itemSup,"d");
    	System.out.println(itemSup.size());
        
    }
    
    public static void addToHashItemSup(HashMap<String, Integer> itemSup, String item){
    	Integer value = 0;
    	
    	if((value = itemSup.get(item)) == null){
    		itemSup.put(item, 1);
    	}else{
    		value++;
    		itemSup.put(item, value);
    	}
    }
    
    public int execEquals(String itemset, ArrayList<String> transactions){
    	//Equals
        for(int i = 0; i < 10000; i++){
        	count = countByEquals(itemset, transactions);
        }
        return countByEquals(itemset, transactions);
    }
    
    public int execEquals2(String itemset, ArrayList<String> transactions){
    	//Equals
        for(int i = 0; i < 10000; i++){
        	countByEquals2(itemset, transactions);
        }
        
        return countByEquals2(itemset, transactions);
    }
    
    public int execMatches(String itemset, ArrayList<String> transactions){
    	//Matches
        for(int i = 0; i < 10000; i++){
        	countByMatches(itemset, transactions);
        }
        
        return countByMatches(itemset, transactions);
    }

	private static ArrayList<String> getTransactions() {
		// TODO Auto-generated method stub
		
		ArrayList<String> t = new ArrayList<String>();
		t.add("1 2 3 4 5");
		t.add("1 2");
		t.add("2 3 4 5");
		t.add("1 5 6");
		t.add("1 3 4 5");
		return t;
	}
}