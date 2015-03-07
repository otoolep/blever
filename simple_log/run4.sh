#!/bin/bash

./simple_log_index -logs ../zoto_sample_logs.log.100k -batchSize 300 -shards 1 -index "logs0.bleve" &
./simple_log_index -logs ../zoto_sample_logs.log.100k -batchSize 300 -shards 1 -index "logs1.bleve" &
./simple_log_index -logs ../zoto_sample_logs.log.100k -batchSize 300 -shards 1 -index "logs2.bleve" &
./simple_log_index -logs ../zoto_sample_logs.log.100k -batchSize 300 -shards 1 -index "logs3.bleve" &
wait
