import sys

def processing():
	ysum=0
	x_int=None
	for line in sys.stdin:
		line=line.strip('\n')
		lines=line.split("\t")
		x=int(lines[0])
		y=float(lines[1])
		if x_int==None:
			x_int=x
		else:
			if x_int!=x:
				print "{}\t{}".format(x_int, ysum)
				ysum=0
				x_int=x
		ysum+=y
	if x_int!=None:
		print "{}\t{}".format(x_int, ysum)


if __name__=="__main__":

	processing()
