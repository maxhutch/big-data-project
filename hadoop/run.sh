#!/bin/bash
rm -rf tmp*
~/src/hadoop-1.1.2/bin/hadoop jar Tiling.jar Tiling bootstrap test tmp0
#~/src/hadoop-1.1.2/bin/hadoop jar Tiling.jar Tiling output tmp0 tmp1

old=tmp0
for i in {1..10} 
do
~/src/hadoop-1.1.2/bin/hadoop jar Tiling.jar Tiling continue $old tmp${i}
old=tmp${i}
done
~/src/hadoop-1.1.2/bin/hadoop jar Tiling.jar Tiling output $old tmp11

