def inputFileName = this.args[0];
println "Arquivo: "+inputFileName
HashMap<String, Integer> hashItem = new HashMap<String,Integer>()

String[] itemLines

public void addItem(String item, HashMap<String, Integer> hashItem){

        Integer v = hashItem.get(item)
        if(v != null){
                v++
        }
        hashItem.put(item,v)
}
new File(inputFileName).eachLine { line, index ->
        itemLines = line.split(" ");

        for(i in 0..itemLines.length-1){
                addItem(itemLines[i], hashItem)
        }
}

StringBuilder sb = new StringBuilder();
String[] name = inputFileName.split("/");
sb.append("@relation "+name[name.length-1])
sb.append("\n\n");
Set<String> keys = hashItem.keySet();
int count = 0;

Integer maior = 0
Integer menor = Integer.MAX_VALUE
ArrayList<Integer> attributes = new ArrayList<Integer>()
for(s in keys){
        count++;
        
        attributes.add(Integer.valueOf(s))
}
Collections.sort(attributes)
for(s in attributes){
        sb.append("@attribute \'"+s+"\' {1,0}\n")
}
println attributes.get(0)+" "+attributes.get(1)
println "Quantidade de itens diferentes "+count
File out = new File(inputFileName+".arff")
sb.append("\n\n");
sb.append("@data")
sb.append("\n\n");
out.append(sb.toString())
new File(inputFileName).eachLine { line, index ->
        itemLines = line.split(" ");
        sb = new StringBuilder()
        sb.append("{")
        for(i in 0..itemLines.length-1){
                if(i == itemLines.length-1){
                        sb.append(attributes.indexOf(Integer.parseInt(itemLines[i]))).append(" ").append("1")        
                }else{
                        sb.append(attributes.indexOf(Integer.parseInt(itemLines[i]))).append(" ").append("1,")
                }
        }
        sb.append("}\n")
        out.append(sb.toString())
}

