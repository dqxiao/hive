import sys
def mapping(x):
	# y=2*x
	result=[]
	result.append(2*x)
	result.append(2*x+1)
	return  result


if __name__=="__main__":

	x=int(sys.argv[1])
	
	print mapping(x)





