FROM python:3.10-slim

RUN apt-get update && apt-get install -y python3 python3-pip net-tools
RUN pip3 install grpcio==1.60.1 grpcio-tools==1.60.1 --break-system-packages

COPY *.py /
COPY *.proto /

EXPOSE 5440
CMD ["python3", "server.py"]
