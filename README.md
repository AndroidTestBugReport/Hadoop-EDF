# Hadoop-EDF

This project includes four parts:
The first three parts are contained in "\Hadoop-EDF\Hadoop-EDF\src\main\java\org\yyw\HadoopEDF\ParallelProcessing"
The forth program is in "\Hadoop-EDF\WholeFileProcessingwithoutSplittinginHadoop\src\main\java\OneMoreExperiment"
1. HadoopEDF: a large scale of EDF files are processed in parallel on local machine and Amazon Web Service(AWS).
   * Set up three arguments for "OrderByCompositeKey" class in "Run Configurations-Arguments". One is input folder, second is output folder, third
     is distributed file containing header information, which is obtained from "DistributedFile" class.
2. Sequentially processing these EDF files
   * Processing small EDF files using "EDFtoJsonyuanyuanforsmallfiles", which has the same configuration as HadoopEDF for AWS EC2 instance.
   * Processing large EDF files using "EDFtoJsonyuanyuanforlargefiles", which requires large instance memory on AWS.
3. Compare results which are physical values of sleep signal data from HadoopEDF and Sequential program.

   * It's simple to compare the results of small EDF files, however, it's complex to compare the results of large EDF files because those physical
     values are split to output for limited string length of sequential program on AWS EC2 instance.
   * Compare results of small files and physical values of large files respectively. The comparison on large files were finished separately 
     because the sequential program split the entire contents of certain channel to several subfile as the limited number of strings on AWS.
     Therefore, we have to pay attention to the comparison on splitting contents.
4. Besides the parallel program of Hadoop-EDF and sequential program, I also did one more experiment for The related works by Jayapandian et al. have used the MapReduce framework to process electrophysiological signals in parallel for epilepsy research.

Note: here, the conversion equation "dc[i] = minInUnits[i] - unitsInDigit[i] * digitalMin[i]" is referred by EDF official website "https://www.edfplus.info/specs/guidelines.html"
The only difference is for another version "dc[i] = maxInUnits[i] - unitsInDigit[i] * digitalMax[i]" referred by matlab website. https://www.mathworks.com/matlabcentral/fileexchange/31900-edfread
Either one is ok.
