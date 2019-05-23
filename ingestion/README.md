# Ingestion

Apache Kafka and kafka-python used.

Multiple producers, producer1.py, producer_pv.py and producer3.py send data to kafka from different reddit.com comment files. Each producer sends data at approximately 700 messages/s.

producer_pv.py send page views data, while others send only comments. As an extension pageviews could be simulated by uniformly sampling from historical views with weights given to age of the views. Page view data was not available in the  reddit dataset and hence was simulated.
