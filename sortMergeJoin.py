import sys
import copy
from collections import OrderedDict
from external_sort import external_sort

class sortMergeJoin:
    def __init__(self, R_path, S_path, R_name, S_name, n_memory_blocks, output_file_name):
        self.R_path = R_path
        self.S_path = S_path
        self.R_name = R_name
        self.S_name = S_name
        self.n_memory_blocks = n_memory_blocks #no. of memory blocks
        self.output_file_name = output_file_name
        self.search_dict = OrderedDict()
        self.BLOCKSIZE = 3
        self.splits1 = 0
        self.splits2 = 0
        self.fptr_R = None
        self.fptr_S = None
        self.R_inter_file_no = 2
        self.S_inter_file_no = 2
        self.fptr_write = None
    def sort(self):
        R_table = external_sort(self.R_path, self.R_name, self.n_memory_blocks, self.BLOCKSIZE)
        splits1 = R_table.partition("first")
        R_table.merge(self.R_name+"sorted", splits1, "first")
        S_table = external_sort(self.S_path, self.S_name, self.n_memory_blocks, self.BLOCKSIZE)
        splits2 = S_table.partition("second")
        S_table.merge(self.S_name+"sorted", splits2, "second")
        
    def split(self):
        output_file_name1 = self.R_name + "sorted"
        output_file_name2 = self.S_name + "sorted"
        R_table_sorted = external_sort("./"+output_file_name1, output_file_name1, self.n_memory_blocks, self.BLOCKSIZE)
        self.splits1 = R_table_sorted.partition("first")
        S_table_sorted = external_sort("./"+output_file_name2, output_file_name2, self.n_memory_blocks, self.BLOCKSIZE)
        self.splits2 = S_table_sorted.partition("second")

    def _DictPopulate(self, row, table_index):
        row = row.split()
        if table_index == "first":
            key = row[1]
            value = row[0]
            if key not in self.search_dict.keys():
                self.search_dict[key] = list()
            self.search_dict[key].append([value, "R"])
        else:
            key = row[0]
            value = row[1]
            if key not in self.search_dict.keys():
                self.search_dict[key] = list()
            self.search_dict[key].append([value, "S"])

    def initDictPopulate(self, temp_1, table_index):
        f = open(temp_1, "r")
        while True:
            line = f.readline()
            if not line:
                break
            self._DictPopulate(line, table_index)
        f.close()

    def bring_next_keys(self,table_index, table_count, table_data):
        count = 0
        if table_index == "first":
            table_inter_file_no = self.R_inter_file_no
            # table_ptr = self.fptr_R
            table_name = self.R_name
            splits = self.splits1
        else:
            table_inter_file_no = self.S_inter_file_no
            # table_ptr = self.fptr_S
            table_name = self.S_name
            splits = self.splits2
        if table_inter_file_no <= splits and len(table_data):         #we did not exhaust sublists and still few tuples need to be matched
            # print("int file R:", self.R_inter_file_no, "int file S:", self.S_inter_file_no)
            while True:
                #get next row from current sublist ptr
                if table_index == "first":
                    line = self.fptr_R.readline()
                else:
                    line = self.fptr_S.readline()
                #check if line is empty, then go to next sublist
                if len(line) == 0:
                    if table_index == "first":
                        self.R_inter_file_no += 1
                        self.fptr_R.close()
                    else:
                        self.S_inter_file_no += 1
                        self.fptr_S.close()
                    if table_inter_file_no == splits+1:
                        break
                    else:
                        if table_index == "first":
                            self.fptr_R = open(table_name+"sorted"+str(table_inter_file_no), "r")
                        else:
                            self.fptr_S = open(table_name+"sorted"+str(table_inter_file_no), "r")
                        break

                self._DictPopulate(line, table_index)

                count += 1
                
                if count == table_count:
                    break

    def getNext(self):
        r_data = list()
        s_data = list()
        r_count = 0
        s_count = 0
        old_first_key = list(self.search_dict.keys())[0]
        # keep fetching records until old and new first key mismatches or dict becomes empty
        while True:

            if len(self.search_dict) == 0:
                break

            new_first_key = list(self.search_dict.keys())[0]
            if old_first_key != new_first_key:
                break
            # print("int file R:", self.R_inter_file_no, "int file S:", self.S_inter_file_no)
            #store in temp lists with common "y", to combine later
            for values in self.search_dict[old_first_key]:
                if values[1] == "R":
                    r_count = r_count + 1
                    r_data.append([values[0], old_first_key])
                else:
                    s_count = s_count + 1
                    s_data.append([old_first_key, values[0]])

            del self.search_dict[old_first_key]

            #look for new records to bring
            self.bring_next_keys("first", r_count, r_data)
            self.bring_next_keys("second", s_count, s_data)
        for r in r_data:
            for s in s_data:
                self.fptr_write.write(str(r[0])+" "+str(r[1])+" "+str(s[1])+'\n')
        


    def join(self):
        # populate dctionary with r1 and s1 keys
        self.initDictPopulate(self.R_name+"sorted"+str(1), "first")
        self.initDictPopulate(self.S_name+"sorted"+str(1), "second")
        # print(self.search_dict)
        # print(self.search_dict.keys())
        print("first_key",list(self.search_dict.keys())[0])

        # open next corresponding sublists (if exists)
        if self.splits1 >= 2:
            self.fptr_R = open(self.R_name+"sorted"+str(2),'r')
            
        if self.splits2 >= 2:
            self.fptr_S = open(self.S_name+"sorted"+str(2),'r')

        self.fptr_write = open(self.output_file_name,"w")
        while len(self.search_dict):
            self.getNext()
    
    def close(self):
        #clean up all intermediate files created
        pass