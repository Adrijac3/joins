import os
class hashJoin:
    def __init__(self, R_path, S_path, R_name, S_name, n_memory_blocks, output_file_name):
        self.R_path = R_path
        self.S_path = S_path
        self.R_name = R_name
        self.S_name = S_name
        self.n_memory_blocks = n_memory_blocks
        self.output_file_name = output_file_name
        self.hash_table = {}
        self.BLOCKSIZE = 100
        self.R_count = [0] * n_memory_blocks
        self.S_count = [0] * n_memory_blocks
        self.fptr_write = None

    def calculateHash(self,k):
        p = 53
        value = 0
        p_power = 1
        key = str(k)
        for c in str(key):
            value=(value+ord(c)*p_power) % self.n_memory_blocks
            p_power=(p_power*p) % self.n_memory_blocks
        return value

    def _hash(self, table_index):
        if table_index == "first":
            table_name = self.R_name
            table_path = self.R_path
        else:
            table_name = self.S_name
            table_path = self.S_path
        try:
            f=open(table_path,"r")
        except IOError:
            print(temp_name + " could not be opened\n")
            sys.exit()
        while True:
            line = f.readline()
            if not line:
                break
            row = line.strip('\n').split()
            # print(row)
            key = ""
            # value = ""
            if table_index == "first":
                key = row[1]
            #     value = row[0]
            else:
                key = row[0]
            #     value = row[1]
            hash_value = self.calculateHash(key)
            if table_index == "first":
                self.R_count[hash_value] += 1
            else:
                self.S_count[hash_value] += 1
            # print(hash_value)
            hash_file_name = table_name+"_"+str(hash_value)

            f1 = open(hash_file_name, "a")
            # print(row)
            f1.write(row[0] + " " + row[1] +"\n")
            f1.close()

        f.close()

    def hash(self):
        #send R to hash
        self._hash("first")
        #send S to hash
        self._hash("second")
        self.fptr_write = open(self.output_file_name, "w")

    def getNext(self,hash_value):
        if self.R_count[hash_value] == 0 or self.S_count[hash_value] == 0:
            return
        small = "second"
        if self.R_count[hash_value] < self.S_count[hash_value]:
            small = "first"
        fptr_small = None
        fptr_big = None

        if small =="first":
            fptr_small = open(self.R_name+"_"+str(hash_value),'r')
            fptr_big = open(self.S_name+"_"+str(hash_value),'r')
        else:
            fptr_small = open(self.S_name+"_"+str(hash_value),'r')
            fptr_big = open(self.R_name+"_"+str(hash_value),'r')
        #check if smaller one fits in memory block

        #store small list in mm
        temp_list = []
        while True:
            line = fptr_small.readline()
            if not line:
                break
            row = line.strip("\n").split()
            temp_list.append([row[0], row[1]])   # x y if small -> R, y x if small -> S
        fptr_small.close()

        #pick each line from big file and linearly scan temp_list to find match

        while True:
            line = fptr_big.readline()
            if not line:
                break
            row = line.strip("\n").split()
            x_r = ""
            y_r = ""
            y_s = ""
            z_s = ""
            for tuples in temp_list:
                if small == "first":
                    x_r, y_r = tuples[0], tuples[1]
                    y_s, z_s = row[0], row[1]
                else:
                    y_s, z_s = tuples[0], tuples[1]
                    x_r, y_r = row[0], row[1]
                if y_r == y_s:
                    self.fptr_write.write(str(x_r) + " " + str(y_r) + " " + str(z_s) + "\n")


            


        

    def join(self):
        for i in range(0, self.n_memory_blocks):
            self.getNext(i)
        self.close()
    
    def close(self):
        self.fptr_write.close()
        # close temp files
        for i in range(0, self.n_memory_blocks):
            if self.R_count[i] != 0:
                os.remove(self.R_name+"_" + str(i))
            if self.S_count[i] != 0:
                os.remove(self.S_name+"_" + str(i))