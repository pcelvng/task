# Stats

The stats utility will attach to the task bus and consume tasks on all topics 
(that transmit task objects). The stats utility can then be queried via 
simple REST calls for general information about the active task ecosystem. 

Provided stats include:

- Average time to complete a task of a particular type
- Error rates (across all tasks types and by task type)
- Error totals (across all task types and by task type)
- Average number of tasks processed per hour (broken down by task type)
- Total tasks completed (broken down by total completed and total error)
- Uptime of the stats utility (since stats state is not recorded anywhere)
- Tasks that were created but don't have a corresponding 'done' record
