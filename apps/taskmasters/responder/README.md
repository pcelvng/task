# Responder Taskmaster

The responder taskmaster will listen to the task bus and respond to 
successfully completed tasks by creating other tasks.

This is useful in launching tasks in series. For example, a task could
be launched by a different taskmaster to get the series started. When
that task completes the responder could create another task who's input
is dependent on the output of the previous task.

