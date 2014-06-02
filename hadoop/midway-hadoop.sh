#!/bin/bash

[[ ! $SLURM_JOB_ID ]] && {
    echo "This script requires an active slurm job, aborting." >&2
    exit 1
}

echo "Configuring Hadoop for Midway"
export HADOOP_MIDWAY=1

# An optional string representing this instance of hadoop. $USER by default.
# export HADOOP_IDENT_STRING=$USER

# The maximum amount of heap to use, in MB.
HADOOP_HEAPSIZE=${HADOOP_HEAPSIZE:-2000}

HADOOP_PREFIX=${HADOOP_PREFIX:-"/scratch/midway/${USER}/hadoop-midway/${SLURM_JOB_ID}"}
HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-${HADOOP_PREFIX}/conf}
HADOOP_LOG_DIR=${HADOOP_LOG_DIR:-${HADOOP_PREFIX}/log}

[ -d $HADOOP_CONF_DIR ] || mkdir -p $HADOOP_CONF_DIR

HADOOP_SSH_OPTS="-i /home/$USER/.ssh/id_dsa-slurm"

export HADOOP_PREFIX HADOOP_CONF_DIR HADOOP_LOG_DIR HADOOP_HEAPSIZE HADOOP_SSH_OPTS

env | grep HADOOP

# Setup the master and slaves
#hostname | sed -e 's/$/-ib0/' >> $HADOOP_PREFIX/conf/masters
#srun hostname | sed -e 's/$/-ib0/' >> $HADOOP_PREFIX/conf/slaves
hostname > $HADOOP_PREFIX/conf/masters
srun hostname | sort | uniq > $HADOOP_PREFIX/conf/slaves

# Setup local hdfs directory
srun mkdir -p /scratch/local/$USER/hadoop-midwa^y/$SLURM_JOB_ID/hds

# Generate configuration files
MASTER=$(hostname)
PORT=$(shuf -i 19000-20000 -n 1)

cat << EOF > $HADOOP_PREFIX/conf/core-site.xml
<configuration>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/scratch/local/$USER/hadoop-midway/$SLURM_JOB_ID/hdfs</value>
    </property>
    <property>
        <name>fs.default.name</name>
        <value>hdfs://$MASTER:$PORT</value>
    </property>
     <property>
         <name>io.sort.mb</name>
         <value>1024</value>
     </property>
     <property>
         <name>io.file.buffer.size</name>
         <value>1048576</value>
     </property>
</configuration>
EOF

cat << EOF > $HADOOP_PREFIX/conf/mapred-site.xml
<configuration>
     <property>
         <name>mapred.job.tracker</name>
         <value>$MASTER:$[ $PORT + 1 ]</value>
     </property>
     <property>
         <name>mapred.tasktracker.map.tasks.maximum</name>
         <value>2</value>
     </property>
     <property>
         <name>mapred.tasktracker.reduce.tasks.maximum</name>
         <value>2</value>
     </property>
     <property>
         <name>io.sort.mb</name>
         <value>1024</value>
     </property>
     <property>
         <name>mapred.map.child.java.opts</name>
         <value>-Xmx2048m</value>
     </property>
     <property>
         <name>mapred.reduce.child.java.opts</name>
         <value>-Xmx2048m</value>
     </property>
     <property>
         <name>mapred.map.tasks.speculative.execution</name>
         <value>false</value>
     </property>
     <property>
         <name>mapred.reduce.tasks.speculative.execution</name>
         <value>false</value>
     </property>
     <property>
         <name>mapred.inmem.merge.threshold</name>
         <value>0</value>
     </property>
     <property>
         <name>mapred.job.reduce.input.buffer.percent</name>
         <value>0.95</value>
     </property>
</configuration>
EOF

cat << EOF > $HADOOP_PREFIX/conf/hdfs-site.xml
<configuration>
     <property>
         <name>dfs.replication</name>
         <value>1</value>
     </property>
     <property>
         <name>dfs.block.size</name>
         <value>134217728</value>
     </property>
</configuration>
EOF
