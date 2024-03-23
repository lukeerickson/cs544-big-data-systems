FROM ubuntu:23.10
# RUN apt-get install wget 
RUN apt-get update && apt-get install -y wget unzip
ADD count.sh /
COPY count.sh /var/run/count.sh

CMD ["./count.sh"] 
