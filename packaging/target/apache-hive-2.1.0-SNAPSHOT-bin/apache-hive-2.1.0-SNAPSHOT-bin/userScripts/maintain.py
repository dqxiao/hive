import sys
import importlib 
import os 
import subprocess 



configurations=[]
module_Map={}
vCount={}
tupleViolation=dict()



class Correlation:


	def __init__(self, confs):
		self.SHC_Name=confs[0]
		self.SHC_SID=int(confs[1])
		self.SHC_DID=int(confs[2])
		self.SHC_GRAN=confs[3]
		self.SHC_MAPFUNC=confs[4]
		self.SHC_INPUT_TYPE =confs[5]
		self.SHC_OUTPUT_TYPE=confs[6]
		self.write_flag=True 

	def __str__(self):
		return "{} {} {} {} {} {} {}".format(self.SHC_Name,self.SHC_SID,
			self.SHC_DID,self.SHC_GRAN,self.SHC_MAPFUNC,
			self.SHC_INPUT_TYPE,self.SHC_OUTPUT_TYPE)
	

	def overBound(self):
		self.write_flag=False



def optionParser(argv):

	size=len(argv)

	for i in range(0,size,7):
		configurations.append(Correlation(argv[i:i+7]))
	# init the original set 
	#
	for cor in configurations:
		name=cor.SHC_Name
		tupleViolation[name]=[] 








methods={'integer':int, 
		  'int': int,
		  'float':float}





def singleton_verify(tline):
	#print tline 
	if tline[0]==tline[1]:
		return True
	else:
		return False

def list_verify(tline):

	if tline[0] in tline[1:-1]:
		return True
	else:
		return False

def range_verify(tline):

	if tline[0]>=tline[1] and tline[0]<=tline[2]:
		return True
	else:
		return False

verifyMethod={'singleton':singleton_verify, 'list': list_verify, 'range':range_verify}

def testViolate(cor, lines):
	inputType=cor.SHC_INPUT_TYPE
	outputType=cor.SHC_OUTPUT_TYPE


	sid=cor.SHC_SID
	did=cor.SHC_DID
	name=cor.SHC_Name
	granlarity=cor.SHC_GRAN

	inputType=inputType.lower()
	outputType=outputType.lower()
	granlarity=granlarity.lower()


	inputVal=methods[inputType](lines[sid])
	resultVal=methods[outputType](lines[did])

	module_obj=module_Map[name]
	mapVal=module_obj.mapping(inputVal)

	tline=[]
	tline.append(resultVal)

	if type(mapVal) is list:
		tline+=mapVal
	else:
		tline.append(mapVal)

	#print tline 
	check=verifyMethod[granlarity](tline)
	oldline='\t'.join(lines)

	if not check:
		tupleViolation[name].append(oldline) # set violated val 
		vCount[name]+=1


def scriptPath():

	mr_local=os.environ["mapreduce_cluster_local_dir"]
	paths=mr_local.split('/')
	paths=paths[:-2]
	return '/'.join(paths)+"/"




def process():

	path=scriptPath()
	script_path=path+"filecache/"
	subdirs=[x[0] for x in os.walk(script_path)]


	for subdir in subdirs:
		sys.path.append(subdir)


	for cor in configurations:
		module_obj=__import__(cor.SHC_Name)
		module_Map[cor.SHC_Name]=module_obj
		vCount[cor.SHC_Name]=0

	for line in sys.stdin:
		line=line.strip()
		lines=line.split('\t') 

		for cor in configurations:
			if vCount[cor.SHC_Name]< threshold:
				testViolate(cor,lines)




	for cor in configurations:
		
		name=cor.SHC_Name
		if vCount[cor.SHC_Name]>=threshold:
			cor.overBound()
		print "{}\t{}".format(cor.SHC_Name,vCount[cor.SHC_Name])

# 
def writeFile(mapreduce_task_id,cor):
	# deal with sepeartely 
	name=cor.SHC_Name
	fileName=name+"_"+mapreduce_task_id
	e_file=open(fileName,'w')

	for item in tupleViolation[name]:
		e_file.write("{}\n".format(item))
	e_file.close()
	return fileName
	



if __name__=="__main__":
	#print "echo"
	optionParser(sys.argv[1:])
	global threshold
 	if "maintain_threshold" in os.environ:
 		threshold=int(os.environ["verify_threshold"])
 	else:
 		threshold=1000
	process()

	# mapreduce_task_id="01"
	mapreduce_task_id=os.environ["mapreduce_task_id"]
	mapreduce_task_id=mapreduce_task_id+".txt"

	#done 
	hadoop_bin_path=os.environ["hadoop_bin_path"]
	for cor in configurations:
		if cor.write_flag:
			fileName=writeFile(mapreduce_task_id,cor)
			name=cor.SHC_Name
			with open(os.devnull, 'wb') as devnull:
				subprocess.check_call([hadoop_bin_path, 'dfs' , '-copyFromLocal', './'+fileName, '/maintain/'+name+"/"], stdout=devnull, stderr=subprocess.STDOUT)
