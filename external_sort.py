import os
import sys
class external_sort:
    def __init__(self, table_path, table_name, n_memory_blocks, block_size):
        self.table_path = table_path
        self.table_name = table_name
        print(n_memory_blocks, block_size)
        self.M = block_size * n_memory_blocks
        print(self.M)
    def _sortIntermediate(self, rows, table_index):
        if table_index == "first":
            rows = sorted(rows, key=lambda x: x[1])
        if table_index == "second":
            rows = sorted(rows, key=lambda x: x[0])
        return rows

    def _writeIntermediate(self, rows, temp_file_name):
        try:
            with open(temp_file_name,"w+") as f2:
                f2.writelines(' '.join(str(value) for value in row) + '\n' for row in rows)
        except:
            print("Error: couldn't write on temporary output file")
            sys.exit()

    def partition(self, table_index):
        try:
            f= open(self.table_path,"r")
        except IOError:
            printError("Error: Input file could not be opened\n")
            sys.exit()
        rows=[]
        count=0
        file_no=1
        temp_file_name = self.table_name + str(file_no)
        while True:
            line= f.readline()
            if not line:
                break
            line=line.strip("\n").split(" ")    
            rows.append(line)
            count+=1
            if(count == self.M):
                # print("upar wala count = ", count)
                print("sorting # " + temp_file_name + " sublist")
                rows = self._sortIntermediate(rows, table_index)
                # print(rows)
                print("Writing to disk # "+ temp_file_name)
                self. _writeIntermediate(rows,temp_file_name)
                rows.clear()
                count = 0
                file_no += 1
                temp_file_name = self.table_name + str(file_no)
        if count>0:
            # print(" extra tuples = ", count)
            print("sorting remaining # " + temp_file_name +" sublist")
            rows = self._sortIntermediate(rows, table_index)
            print("Writing to disk # "+ temp_file_name)
            self._writeIntermediate(rows,temp_file_name)
            file_no += 1
        f.close()
        return file_no - 1

    def merge(self, output_file_name, n_of_sublists, table_index):
        print("Sorting...")
        temp_file_ptrs=[]     #list of file pointers to store temp file descriptors
        min_tuples=[]        #list to store minimum tuple from each temp file and its filedescriptor

        for i in range(1,n_of_sublists+1):
            try:
                temp_file_name = self.table_name + str(i)
                f=open(temp_file_name,"r")
            except IOError:
                print(temp_file_name + " could not be opened\n")
                sys.exit()
            temp_file_ptrs.append(f)
            line = f.readline()
            if not line:
                break
            line = line.split()
            min_tuples.append([line,f])
        with open(output_file_name,"w+") as f2:
            print("Writing to disk")
            while len(min_tuples)>0:
                if table_index=="first":
                    fn = lambda x: [x[0][1]]
                elif table_index=="second":
                    fn = lambda x: [x[0][0]]
                smallest=min(min_tuples,key= fn)
                f2.write(' '.join(smallest[0])+'\n')
                fptr=smallest[1]
                min_tuples.remove(smallest)
                line=fptr.readline()
                if not line:
                    continue
                min_tuples.append([line.split(),fptr])
        print("###completed execution")

        #close file pointers and remove temp files
        for i in range(1,len(temp_file_ptrs)):
            temp_file_ptrs[i].close()
        for i in range(1,n_of_sublists+1):
            temp_file_name = self.table_name + str(i)
            os.remove(temp_file_name)
