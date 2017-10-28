# Retry Taskmaster

The retry taskmaster will listen on the task bus for failed tasks and when 
it receives a failed task will apply any retry rules for that task type.

The retry taskmaster is capable of keeping in-memory state of the particular
task current retry count so that it can apply the retry rule correctly.

If the retry taskmaster is restarted then the in-memory task retry state 
is forgotten.

Once a task is successful, the retry taskmaster will log the number
of failed attempts before success and then clear the task retry state 
for that task.

