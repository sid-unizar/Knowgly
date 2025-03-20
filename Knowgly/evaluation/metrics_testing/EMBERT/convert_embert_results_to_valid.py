import sys


output = open(sys.argv[1]+"_valid.run", 'w')

with open(sys.argv[1]) as file:
    lines = len(file.readlines())

with open(sys.argv[1]) as file:
    while line := file.readline():
        line = line.rstrip() # remove trailing spaces and '\n'
        line = line.replace("\t", " Q0 ", 1) # replace first tab with Q0 and spaces
        line = line.replace("\t", " ") # And the rest with just spaces
        
        
        output.write(line.replace('\t', ' ') + " " + str(float(lines)) + " reranking\n")
        lines -= 1

