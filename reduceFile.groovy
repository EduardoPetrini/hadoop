def inputFileName = this.args[0];
println "Arquivo: "+inputFileName

File out = new File(inputFileName+".small")

new File(inputFileName).eachLine { line, index ->
		if((index%3)!=0){
			out.append(line)
			out.append("\n")
		}
}
