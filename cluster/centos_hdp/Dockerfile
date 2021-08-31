FROM centos7_base
#解压下载的java文件
ADD jdk-8u221-linux-x64.tar.gz /usr/local/
# 更改文件夹名字
RUN mv /usr/local/jdk1.8.0_221 /usr/local/jdk1.8
ENV JAVA_HOME /usr/local/jdk1.8
ENV PATH $JAVA_HOME/bin:$PATH
# 解压下载的Hadoop文件
ADD hadoop-3.1.3.tar.gz /usr/local
RUN mv /usr/local/hadoop-3.1.5 /usr/local/hadoop
ENV HADOOP_HOME /usr/local/hadoop
ENV PATH $HADOOP_HOME/bin:$PATH