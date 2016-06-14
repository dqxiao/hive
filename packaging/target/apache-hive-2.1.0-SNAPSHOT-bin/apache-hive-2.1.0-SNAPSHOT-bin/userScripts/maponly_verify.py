import sys
import importlib 
import os 
import subprocess 
from ConfigParser import SafeConfigParser
import random



configurations=[]
module_Map={}
vCount={}
distViolation=dict()
mapOutputPercentage=100


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


def jobConfiguration(filepath):
	config = SafeConfigParser()
	config.read(filepath)

	# return config.get('mapReduce', 'map')
	return config



def optionParser(argv):

	size=len(argv)
	#
	if(size%7!=0):
		global mapOutputPercentage
		mapOutputPercentage=int(argv[-1])
		#print "mapP\t{}".format(mapOutputPercentage)
		size-=1
 

	for i in range(0,size,7):
		configurations.append(Correlation(argv[i:i+7]))
	# init the original set 
	for cor in configurations:
		name=cor.SHC_Name
		distViolation[name]=set()








methods={'integer':int, 
		  'int':int, 
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
	if not check:
		distViolation[name].add(inputVal)
		vCount[name]+=1

def scriptPath():

	mr_local=os.environ["mapreduce_cluster_local_dir"]
	paths=mr_local.split('/')
	paths=paths[:-2]
	return '/'.join(paths)+"/"



def runMapScript(mapScripName,mapFields,lines):

	mapFields=mapFields.split(",")
	mapFields=[int(item) for item in mapFields]


	module_obj=module_Map[mapScripName]

	targetLine=[lines[item] for item in mapFields]

	#targetLine='\t'.join(targetLine)
	rVal=random.randint(0,100)
	#print "mapP\t{}".format(mapOutputPercentage)
	if mapOutputPercentage> rVal:
		module_obj.mapping(targetLine)


def writeViolationSummary():
	mapreduce_task_id=os.environ["mapreduce_task_id"]
	vsumFile=mapreduce_task_id+'_sum'+".txt"

	e_file=open(vsumFile,'w')

	for cor in configurations:
		s="{}\t{}\n".format(cor.SHC_Name,vCount[cor.SHC_Name])
		e_file.write(s)

		name=cor.SHC_Name
		if len(distViolation[name])> threshold:
			cor.overBound()
	return vsumFile



def process():

	path=scriptPath()
	script_path=path+"filecache/"
	subdirs=[x[0] for x in os.walk(script_path)]
	jobConfig=None
	# 
	for subdir in subdirs:
		sys.path.append(subdir)
		# I need find the jobConfig.ini
		for fileName in os.listdir(subdir):
			if fileName.endswith('.ini'):
				jobConfig=jobConfiguration(os.path.join(subdir,fileName))

	#done 



	for cor in configurations:
		module_obj=__import__(cor.SHC_Name)
		module_Map[cor.SHC_Name]=module_obj
		vCount[cor.SHC_Name]=0


	mapScripName=jobConfig.get('mapReduce', 'map')
	mapFields=jobConfig.get('mapReduce', 'mapKeys')
	module_obj=__import__(mapScripName)
	module_Map[mapScripName]=module_obj





	for line in sys.stdin:
		line=line.strip()
		lines=line.split('\t') # seperate by ','
		runMapScript(mapScripName,mapFields,lines)

		for cor in configurations:
			testViolate(cor,lines)
			






# 
def writeFile(mapreduce_task_id,cor):
	# deal with sepeartely 
	name=cor.SHC_Name
	fileName=name+"_"+mapreduce_task_id
	e_file=open(fileName,'w')
	count=0

	for item in distViolation[name]:
		e_file.write("{}\n".format(item))
		if(count==threshold):
			break
		count+=1
	
	e_file.close()
	return fileName
	



if __name__=="__main__":
	#print "echo"
	optionParser(sys.argv[1:])
	global threshold
 	if "verify_threshold" in os.environ:
 		threshold=int(os.environ["verify_threshold"])
 	else:
 		threshold=1000
	process()

	
	mapreduce_task_id=os.environ["mapreduce_task_id"]
	hadoop_bin_path=os.environ["hadoop_bin_path"]

	#print "mapP\t{}".format(mapOutputPercentage)


	vsumFile=writeViolationSummary()

	with open(os.devnull, 'wb') as devnull:
		subprocess.check_call([hadoop_bin_path, 'dfs' , '-copyFromLocal', './'+vsumFile, '/tempViolation/'+"summary"+"/"], stdout=devnull, stderr=subprocess.STDOUT)


	#Thanks 
	for cor in configurations:
		fileName=writeFile(mapreduce_task_id,cor)
		name=cor.SHC_Name
		with open(os.devnull, 'wb') as devnull:
			subprocess.check_call([hadoop_bin_path, 'dfs' , '-copyFromLocal', './'+fileName, '/tempViolation/'+name+"/"], stdout=devnull, stderr=subprocess.STDOUT)

