FROM centos

MAINTAINER codingstory
#安装SSH
RUN yum install -y openssh-server sudo

RUN sed -i 's/UsePAM yes/UsePAM no/g' /etc/ssh/sshd_config
RUN yum install -y openssh-clients

#添加用户名为hadoop用户密码为hadoop的用户
RUN useradd hadoop
RUN echo "hadoop:hadoop" | chpasswd

#提升hadoop用户的权限
RUN echo "hadoop   ALL=(ALL)     ALL" >> /etc/sudoers
RUN ssh-keygen -t dsa -f /etc/ssh/ssh_host_dsa_key
RUN ssh-keygen -t rsa -f /etc/ssh/ssh_host_rsa_key

#开启SSH服务端口为22
RUN mkdir /var/run/sshd

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]