import sys


def mapping(lines):
	print "{}\t{}".format(lines[0],lines[1])

def processing():

	for line in sys.stdin:
		line=line.strip('\n')
		lines=line.split('\t')
		mapping(lines)


if __name__=="__main__":
	processing()

