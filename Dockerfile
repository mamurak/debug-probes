FROM registry.access.redhat.com/ubi8/python-39

RUN pip install requests confluent-kafka
ADD debug_runtime.py .

CMD ["sleep", "100000"]
