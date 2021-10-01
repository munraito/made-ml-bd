## HW1: HDFS & MapReduce

cluster screenshots:
`namenode.png, resourcemanager.png`

1st task, HDFS commands:
`hdfs cmd.txt`

input data:
`prices.csv`

MR scripts: `mapper_*.py, reducer_*.py`

run MR jobs:
```bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -files mapper_mean.py,reducer_mean.py -mapper mapper_mean.py -reducer reducer_mean.py -input /hw1/prices.csv -output /hw1/output && hdfs dfs -cat /hw1/output/*
```

```bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -files mapper_var.py,reducer_var.py -mapper mapper_var.py -reducer reducer_var.py -input /hw1/prices.csv -output /hw1/output2 && hdfs dfs -cat /hw1/output2/*
```

results of MR jobs vs standard approach:
`mr_results.txt`
