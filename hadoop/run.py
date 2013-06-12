#!/usr/bin/python3
from os import system, path
from time import time
from sys import argv
from tempfile import mkdtemp

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

rundir = mkdtemp(prefix='runs/',dir=".")
with open(path.join(rundir, "foo"),"w") as f:
  f.write(str(a) + " " + str(b) + " " + str(c) + " " + str(d))

system("hadoop dfs -copyFromLocal "+rundir+" "+hdfs+"input")

times = []; cpu_time = [];
maps = []; reduces = []; 
read = []; written = []; 
map_irecs = []; map_orecs = []
com_irecs = []; com_orecs = []
red_irecs = []; red_orecs = []
spills = [];
system(hadoop_part + " bootstrap "+hdfs+"input "+hdfs+"tmp0 "+str(n))

with open(path.join(rundir,"stats.dat"), 'w') as f:
  f.write("%4s  %9s  %12s  %16s  %16s  %5s  %5s  %10s  %10s  %10s  %10s  %10s  %10s  %10s \n" % ("It", "Time", "CPU Time", "ReadIO", "WriteIO", "MTask", "RTask", "MapInRec", "MapOutRec", "ComInRec", "ComOutRec", "RedInRec", "RedOutRec", "Spills") )

for i in range(a*b + a*c + b*c-2):
  start = time()
  system(hadoop_part + " continue "+hdfs+"tmp"+str(i)+" "+hdfs+"tmp"+str(i+1)+ " " + str(n) + " &> " + path.join(rundir,"tmp.log"))
  times.append(time()-start)
  with open(path.join(rundir,"tmp.log"),"r") as f:
    lines = f.readlines()
    for line in lines:
      parts = line.partition("=")
      if "Launched map tasks" in parts[0]:
        maps.append(int(parts[2]))
      elif "Launched reduce tasks" in parts[0]:
        reduces.append(int(parts[2]))
      elif "Bytes Written" in parts[0]:
        written.append(int(parts[2]))
      elif "Bytes Read" in parts[0]:
        read.append(int(parts[2]))
      elif "Map input records" in parts[0]:
        map_irecs.append(int(parts[2]))
      elif "Map output records" in parts[0]:
        map_orecs.append(int(parts[2]))
      elif "Combine input records" in parts[0]:
        com_irecs.append(int(parts[2]))
      elif "Combine output records" in parts[0]:
        com_orecs.append(int(parts[2]))
      elif "Reduce input records" in parts[0]:
        red_irecs.append(int(parts[2]))
      elif "Reduce output records" in parts[0]:
        red_orecs.append(int(parts[2]))
      elif "CPU time spent (ms)" in parts[0]:
        cpu_time.append(int(parts[2]))
      elif "Spilled Records" in parts[0]:
        spills.append(int(parts[2]))
  system("cat "+path.join(rundir,"tmp.log"))
  with open(path.join(rundir,"stats.dat"),"a") as f:
    f.write("%4d  %9.2f  %12.2f  %16d  %16d  %5d  %5d  %10d  %10d  %10d  %10d  %10d  %10d  %10d \n" % (i+1, times[-1], cpu_time[-1]/1000., read[-1], written[-1], maps[-1], reduces[-1], map_irecs[-1], map_orecs[-1], com_irecs[-1], com_orecs[-1], red_irecs[-1], red_orecs[-1], spills[-1])) 
  system("hadoop dfs -copyToLocal "+hdfs+"tmp"+str(i+1)+"/_logs "+path.join(rundir,"log"+str(i)))
  system("cp "+path.join(rundir,"tmp.log")+" "+path.join(rundir,"log"+str(i)+"/log"))

system(hadoop_part + " output "+hdfs+"tmp"+str(a*b+a*c+b*c-2)+" "+hdfs+"out " + str(n))
system("hadoop dfs -copyToLocal "+hdfs+"out "+path.join(rundir,"out"))
with open(path.join(rundir,"out/part-r-00000"), "r") as f:
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


system("hadoop dfs -copyToLocal "+hdfs+"out "+path.join(rundir,"out"))

