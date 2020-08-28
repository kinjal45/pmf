FROM ubuntu:18.04
RUN apt-get update && \
    apt-get install -y python python-pip && \
    mkdir -p /home/pmf/vendor_allocation_module/
COPY requirements.txt /home/pmf/
RUN pip install -r /home/pmf/requirements.txt

COPY run_allocation.sh /
COPY util.py /home/pmf/vendor_allocation_module/
COPY s3-nsn-franklinpark /home/pmf
COPY gc_allocation.py /home/pmf/vendor_allocation_module/
COPY va_data_processing.py /home/pmf/vendor_allocation_module/
RUN chmod 755 /run_allocation.sh
ENV PYTHONPATH "/usr/bin/python"
ENTRYPOINT
