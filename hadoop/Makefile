JAVAC = javac
CLSPTH = hadoop-core-1.1.2.jar:commons-cli-1.2.jar

Tiling: Tiling.java
	$(JAVAC) -classpath $(CLSPTH) Tiling.java
	jar cvf Tiling.jar *.class

WordCount: WordCount.java
	$(JAVAC) -classpath $(CLSPTH) WordCount.java
	jar cvf WordCount.jar Word*.class

run: 
	rm -rf tmp*
	~/src/hadoop-1.1.2/bin/hadoop jar Tiling.jar Tiling bootstrap test tmp0
