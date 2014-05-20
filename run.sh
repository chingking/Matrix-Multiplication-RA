rm -f *.class matrixFormat/*.class
rm -f *.jar matrixFormat/*.jar

#export HADOOP_HOME=/usr/lib/hadoop/client-0.20
#export HADOOP_HOME=/home/king/hadoop-1.2.1
#export HAMA_HOME=/home/NRLAB/Downloads/hama-trunk
export HADOOP_HEAPSIZE=2048

javac -classpath `hadoop classpath` matrixFormat/*.java *.java
jar cvf nativeMatMult.jar *.class matrixFormat/*.class

#export machines=cloud2.iis.sinica.edu.tw

#for i in 3 4 5 6 7 8 9 10
#do
#	export machines=$machines,cloud$i.iis.sinica.edu.tw
#done

#for dim in 2000

#node as memory-constaint
#node=300;
method="OPB";
for mem in 256
do
#/home/king/Rmpi_workspace/sshsudo -u king cloud$node.iis.sinica.edu.tw service hadoop-0.20-mapreduce-tasktracker start
for (( dim=6000; dim<=10000; dim=dim+1000 ))
do
	sparsity=0
	#for  (( sparsity=0; sparsity<=90; sparsity=sparsity+10 ))  
		#size=`hadoop fs -ls input/mat_${dim}_${sparsity}.A.txt | awk '{print $5}'`
		#if [ $size -le 0 ]; then
			hadoop fs -rm -r input/*
			hadoop fs -rm -r output/*
			/home/king/MatMul_COMP/sample/genSpaMat $dim $dim $sparsity 0 > /home/king/MatMul_COMP/iterative/nativeMatMult/mat_${dim}_$sparsity.txt
			filename=/home/king/MatMul_COMP/iterative/nativeMatMult/mat_${dim}_$sparsity.txt
			size=`ls -l $filename | awk '{print $5}'`
			while [ $size -le 0 ] 
			do
				rm /home/king/MatMul_COMP/iterative/nativeMatMult/mat_${dim}_$sparsity.txt
				/home/king/MatMul_COMP/sample/genSpaMat $dim $dim $sparsity 0 > /home/king/MatMul_COMP/iterative/nativeMatMult/mat_${dim}_$sparsity.txt
				size=`ls -l $filename | awk '{print $5}'`
			done
			hadoop fs -D  dfs.block.size=134217728 -put -f /home/king/MatMul_COMP/iterative/nativeMatMult/mat_${dim}_$sparsity.txt input/mat_${dim}_$sparsity.A.txt
			hadoop fs -D dfs.block.size=134217728 -put -f /home/king/MatMul_COMP/iterative/nativeMatMult/mat_${dim}_$sparsity.txt input/mat_${dim}_$sparsity.B.txt
			rm /home/king/MatMul_COMP/iterative/nativeMatMult/mat_${dim}_$sparsity.txt
		#fi
		method="OPB";
		node=`hadoop job -list-active-trackers | awk 'BEGIN{ count=0; } { count++;  } END{ print count }'`;
		hadoop jar nativeMatMult.jar nativeMatMult input/mat_${dim}_$sparsity.A.txt input/mat_${dim}_$sparsity.B.txt output/mat_${dim}_$sparsity $dim $dim $method $mem $node >> /home/king/MatMul_COMP/iterative/nativeMatMult/results/$method/mat_${dim}_$mem
		method="IPB";
		hadoop jar nativeMatMult.jar nativeMatMult input/mat_${dim}_$sparsity.A.txt input/mat_${dim}_$sparsity.B.txt output/mat_${dim}_$sparsity $dim $dim $method $mem $node >> /home/king/MatMul_COMP/iterative/nativeMatMult/results/$method/mat_${dim}_$mem
	done
done

#hadoop fs -rm -r output/
#hadoop jar nativeMatMult.jar nativeMatMult input/mat_1000_0.txt input/mat_1000_0.txt output/mat_1000_0 1000 1000 20 20
#hadoop jar nativeMatMult.jar nativeMatMult input/in.txt input/in2.txt output/in 4 5
