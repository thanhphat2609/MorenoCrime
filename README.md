# Moreno Crime Analysis with Hadoop

## Description

This is a simple project for analysis crime in moreno using Hadoop .


## Architecture 



## Installation

1. Clone the repository:

```bash
https://github.com/thanhphatuit/MorenoCrime.git
Batch Processing: cd BatchProcessing
Streaming Processing: cd StreamingProcessing
```

2. Start Hadoop:

```bash
hdfs namenode -format
start-all.sh
```

3. About Dataset
It is a graph dataset.


## File Structure

- `BatchProcessing.py`: File for process batch data.
- `Python_Schedule.py`: File for schedule streaming data to Kafka.
- `StreamingProcessing.py`: File for process streaming data from Kafka topic and real time visualization.
- `flume_stream_kafka.conf`: Subscribe to Kafka topic for streaming data

## Video demo
- BatchProcessing: .
- StreamingProcessing: .
- Visualization: .

Feel free to explore and enhance the project as needed!
