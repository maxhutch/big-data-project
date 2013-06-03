#!/usr/bin/python3
from os import system
from time import time
from sys import argv

if len(argv) == 2:
  a = b = c = d = int(argv[1])
else:
  a = int(argv[1]); b = int(argv[2])
  c = int(argv[3]); d = int(argv[4])
if len(argv) >= 6:
  exe = argv[5]
else:
  exe = "~/src/hadoop-1.1.2/bin/hadoop"
if len(argv) >= 7:
  n = int(argv[6])
else:
  n = 1

hadoop_part = exe + " jar Tiling.jar Tiling"
#hdfs = "/scratch/local/"
hdfs = "/scratch/local/"

with open("test/foo", "w") as f:
  f.write(str(a) + " " + str(b) + " " + str(c) + " " + str(d))
system("hadoop dfs -copyFromLocal test "+hdfs)


system("rm -rf tmp* out status")
times = []
start = time()
system(hadoop_part + " bootstrap "+hdfs+"test "+hdfs+"tmp0 "+str(n))
times.append(time()-start)
with open("status","a") as f:
  f.write(str(times[-1])+'\n')
for i in range(a*b + a*c + b*c-2):
  start = time()
  system(hadoop_part + " continue "+hdfs+"tmp"+str(i)+" "+hdfs+"tmp"+str(i+1)+ " " + str(n))
  times.append(time()-start)
  with open("status","a") as f:
    f.write(str(times[-1])+'\n')

start = time()
system(hadoop_part + " output "+hdfs+"tmp"+str(a*b+a*c+b*c-2)+" "+hdfs+"out " + str(n))
times.append(time()-start)
with open("status","a") as f:
  f.write(str(times[-1])+'\n')
system("hadoop dfs -copyToLocal "+hdfs+"out out")


with open("out/part-r-00000", "r") as f:
  lines = f.readlines()
  chunks = lines[0].rpartition("[")  
  toks = chunks[2].split(", ")
  vals = []
  for i in range(len(toks)-1):
    vals.append(int(toks[i]))
  chunks = lines[1].rpartition("[")  
  toks = chunks[2].split(", ")
  for i in range(len(toks)-1):
    vals[i] = vals[i] + int(toks[i])

print(*vals, sep=", ", end="\n")
for i in range(len(times)):
  print("%5.2f, " % (times[i]), end="")
print(sum(times))

