run = "EGamma2018D"
rawFileName = "data/"+run+"_eos.txt"
rawFile = open(rawFileName)
rawContent = rawFile.readlines()
nFiles = len(rawContent)
print("Input file has "+str(nFiles)+" files")

nFilesPerJobs = 20

i = 1
loop = True
while loop:
	fileName = "data/"+run+"_"+str(i)+".json"
	with open(fileName, "w") as f:
		f.write("{\n")
		f.write("\""+run+"\": [\n")
	for j in range(nFilesPerJobs):
		index = (i-1)*nFilesPerJobs + j
		if index >= nFiles:
			loop = False
			break
		else:
			with open(fileName, "a") as f:
				#print(i,j,index)
				if j == nFilesPerJobs-1 or index == nFiles-1: f.write(rawContent[index])
				else: f.write(rawContent[index].strip("\n")+",\n")
	with open(fileName, "a") as f:
		f.write("          ]\n")
		f.write("}")

	i += 1
