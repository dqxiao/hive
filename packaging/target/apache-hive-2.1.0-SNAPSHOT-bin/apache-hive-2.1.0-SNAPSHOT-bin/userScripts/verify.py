import sys
import importlib 
import os 


configurations=[]
module_Map={}
vCount={}

class Correlation:


	def __init__(self, confs):
		self.SHC_Name=confs[0]
		self.SHC_SID=int(confs[1])
		self.SHC_DID=int(confs[2])
		self.SHC_GRAN=confs[3]
		self.SHC_MAPFUNC=confs[4]
		self.SHC_INPUT_TYPE =confs[5]
		self.SHC_OUTPUT_TYPE=confs[6]

	def __str__(self):
		return "{} {} {} {} {} {} {}".format(self.SHC_Name,self.SHC_SID,
			self.SHC_DID,self.SHC_GRAN,self.SHC_MAPFUNC,
			self.SHC_INPUT_TYPE,self.SHC_OUTPUT_TYPE)


def optionParser(argv):

	size=len(argv)

	for i in range(0,size,7):
		configurations.append(Correlation(argv[i:i+7]))

	# just for debugging 

	# for cor in configurations:
	# 	print  cor





methods={'integer':int, 
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

verifyMethod={'singleton':singleton_verify, 'list': list_verify, 'rangle':range_verify}

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
		vCount[name]+=1



def process():

	
	#hive_home_conf="HIVE_HOME"
	#hive_home=os.environ[hive_home_conf]

	hive_home="/Users/dongqingxiao/Documents/vldbProject/hive/packaging/target/apache-hive-2.1.0-SNAPSHOT-bin/apache-hive-2.1.0-SNAPSHOT-bin"



	sys.path.append(hive_home+"/userScripts/")

	for cor in configurations:
		module_obj=__import__(cor.SHC_Name)
		module_Map[cor.SHC_Name]=module_obj
		vCount[cor.SHC_Name]=0

	for line in sys.stdin:
		line=line.strip()
		lines=line.split('\t') # seperate by ','

		for cor in configurations:
			testViolate(cor,lines)

		# Please help 


	for cor in configurations:
		print "{}\t{}".format(cor.SHC_Name,vCount[cor.SHC_Name])
	#





		



if __name__=="__main__":
	#print "echo"
	optionParser(sys.argv[1:])
	process()
	





