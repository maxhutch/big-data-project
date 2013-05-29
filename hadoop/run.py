#!/usr/bin/python3
from os import system
from time import time
from sys import argv

if len(argv) == 2:
  a = b = c = d = int(argv[1])
else:
  a = int(argv[1]); b = int(argv[2])
  c = int(argv[3]); d = int(argv[4])

hadoop_part = "~/src/hadoop-1.1.2/bin/hadoop jar Tiling.jar Tiling"

with open("test/foo", "w") as f:
  f.write(str(a) + " " + str(b) + " " + str(c) + " " + str(d))

system("rm -rf tmp* out")
times = []
start = time()
system(hadoop_part + " bootstrap test tmp0")
times.append(time()-start)
for i in range(a*b + a*c + b*c-2):
  start = time()
  system(hadoop_part + " continue tmp"+str(i)+" tmp"+str(i+1))
  times.append(time()-start)
start = time()
system(hadoop_part + " output tmp"+str(a*b+a*c+b*c-2)+" out")
times.append(time()-start)

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

