import sys

threshold=100
distViolation=set()

methods={'integer':int, 
		  'float':float}
class ViolationConf:
	def __init__(self, confs):
		self.SHC_Name=confs[0]
		self.SHC_OUTPUT_TYPE=confs[1]



def optionParser(argv):
	# parse the options to work 
	# option: 
	#print argv
	global vconf
	vconf=ViolationConf(argv)


def process():
	
	inputType=vconf.SHC_OUTPUT_TYPE
	inputType=inputType.lower()
	SHC_Name=vconf.SHC_Name
	for line in sys.stdin:
		line=line.strip()
		val=methods[inputType](line)
		distViolation.add(val)


	if len(distViolation)<threshold:
		for item in distViolation:
			print "{},{}".format(SHC_Name, item)


if __name__=="__main__":
	optionParser(sys.argv[1:])
	process()
