import sys
import os
from sortMergeJoin import sortMergeJoin
from hashJoin import hashJoin

def printError(msg):
    print("\033[91m {}\033[00m".format(msg))
    print()

def getFileName(file_path):
    file_name = os.path.basename(file_path)
    return file_name

def countTuples(file_path):
    try:
        f= open(file_path,"r")
    except IOError:
        printError("Error: Input file of given name not found.\n")
        sys.exit()
    n_tuples=0
    while True:
        l= f.readline()
        if not l:
            break
        n_tuples+=1
    f.close()
    return n_tuples

def main():
    if len(sys.argv) != 5:
        printError("Error: syntax to run program: python3 <drive.py> <path of R file> <path of S file> <sort/hash> <M>")
        sys.exit()

    # extract info passed in CLA
    R_path = sys.argv[1]
    S_path = sys.argv[2]
    algorithm = sys.argv[3]
    n_memory_blocks = int(sys.argv[4])
    BLOCKSIZE = 3
    # print(R_path, S_path, algorithm, n_memory_blocks)

    #get file name of the tables
    R_name = getFileName(R_path)
    S_name = getFileName(S_path)

    #get tuple size of the tables
    R_n_tuples = countTuples(R_path)
    S_n_tuples = countTuples(S_path)

    #make output file name as given in documentation
    output_file_name = R_name + "_" + S_name + "_" + "join.txt"

    if algorithm == "sort":
        #check constraint
        if ((R_n_tuples / BLOCKSIZE) + (S_n_tuples / BLOCKSIZE)) >= n_memory_blocks**2:
            printError("Insufficient memory blocks")
            sys.exit()
        #create sortMergeJoin object, initialize it and call relevant functions
        smj = sortMergeJoin(R_path, S_path, R_name, S_name, n_memory_blocks, output_file_name)
        smj.sort()
        smj.split()
        smj.join()

    elif algorithm == "hash":
        #check constraint
        if min((R_n_tuples / BLOCKSIZE),(S_n_tuples / BLOCKSIZE)) >= n_memory_blocks**2:
            printError("Insufficient memory blocks")
            sys.exit()
        #create hashJoin object, initialize it and call relevant functions
        hj = hashJoin(R_path, S_path, R_name, S_name, n_memory_blocks, output_file_name)
        hj.hash()
        hj.join()

    

main()