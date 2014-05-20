#/bin/sh
rm -f *.class matrixFormat/*.class
rm -f *.jar

#export HADOOP_HOME=/home/king/hadoop-1.2.1
export MAHOUT_HOME=/home/king/mahout/trunk/core/target
#export HAMA_HOME=/home/NRLAB/Downloads/hama-trunk
#export HADOOP_MAPRED_HOME=/usr/lib/hadoop-mapreduce
export HADOOP_HEAPSIZE=2048

javac -classpath `hadoop classpath` matrixFormat/*.java *.java
jar cvf nativeMatMult.jar *.class matrixFormat/*.class

dim=$1;
sparsity=0;
mem=256;
gen=0;
method="IPB";
if [ $gen -eq 1 ]; then
	/home/king/MatMul_COMP/sample/genSpaMat $dim $dim $sparsity 0 > /home/king/MatMul_COMP/iterative/nativeMatMult/mat_${dim}_$sparsity.txt
	filename=/home/king/MatMul_COMP/iterative/nativeMatMult/mat_${dim}_$sparsity.txt
	size=`ls -l $filename | awk '{print $5}'`
	while [ $size -le 0 ] 
	do
		rm /home/king/MatMul_COMP/iterative/nativeMatMult/mat_${dim}_$sparsity.txt
		/home/king/MatMul_COMP/sample/genSpaMat $dim $dim $sparsity 0 > /home/king/MatMul_COMP/iterative/nativeMatMult/mat_${dim}_$sparsity.txt
		size=`ls -l $filename | awk '{print $5}'`		
	done
	hadoop fs -D dfs.block.size=134217728 -put -f /home/king/MatMul_COMP/iterative/nativeMatMult/mat_${dim}_$sparsity.txt input/mat_${dim}_$sparsity.A.txt
	hadoop fs -D dfs.block.size=134217728 -put -f /home/king/MatMul_COMP/iterative/nativeMatMult/mat_${dim}_$sparsity.txt input/mat_${dim}_$sparsity.B.txt
	rm /home/king/MatMul_COMP/iterative/nativeMatMult/mat_${dim}_$sparsity.txt
fi
hadoop fs -rm -r output/*
#mat_${dim}_$sparsity
#hadoop fs -rm -r input/*.trans*
#hadoop jar nativeMatMult.jar nativeMatMult input/in.txt input/in2.txt output/tin 4 4 $method 100
#hadoop jar nativeMatMult.jar nativeMatMult input/mat_${dim}_$sparsity.A.txt input/mat_${dim}_$sparsity.B.txt output/mat_${dim}_$sparsity $dim $dim naive
n=`hadoop job -list-active-trackers | awk 'BEGIN{ count=0; } { count++;  } END{ print count }'`
hadoop jar nativeMatMult.jar nativeMatMult input/mat_${dim}_$sparsity.A.txt input/mat_${dim}_$sparsity.B.txt output/mat_${dim}_$sparsity $dim $dim $method $mem $n
