# Noop Worker 

The noop worker doesn't do anything except listen for tasks and mark the task
as a success or failure at random. The failure rate can be set at runtime. The
noop worker can simulate working on the task for a period of time by setting a 
task completion length or length range.