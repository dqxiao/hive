import sys
def mapping(x):
	y=2*x

	return y 


if __name__=="__main__":

	for line in sys.stdin:
		line=line.strip()
		x=int(line)
		print mapping(x)





