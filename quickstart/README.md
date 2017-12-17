# Quickstart

## Part 1 - Make Worker

Get started by making your first worker by following the instructions found in the quickstart/worker/README.md page.

## Part 2 - Make Taskmaster

Get started by making your first worker by following the instructions found in the quickstart/taskmaster/README.md page.

## Part 3 - Taskmaster and Worker

With having built a taskmaster and worker you are ready to put the two together! In this directory run the following 
command:

```bash
$ ./taskmaster/taskmaster -type='hello-world' -info='Hello World!' | ./worker/worker > done.json 
```

You should see output similar to the first worker exercise. In this simple example, the taskmaster you built writes 
to stdout and the worker reads from stdin. The task bus in this case is simple stdin/stdout io. Of course using stdio 
will not likely be your production choice it's great for simple testing. Task provides out-of-the-box support for a 
number of popular message buses that can be used as your task bus.

## Part 3 - Growing Up

Now that you have made a basic worker and taskmaster you are ready grow up and move on.
A whole world of task-goodness awaits you! 

We hope you now see that fundamentally Task is very simple. You have Taskmasters that create tasks - signals for a worker 
to do something. Then there are Workers - little programs that listen for tasks on the task bus and then complete those
tasks.

The Task framework (if you could call it that) is fundamentally simple but building a full, production-quality task 
ecosystem will require implementing Task intelligently and in a way that works for your problems. To guide you, we have
prepared some basic philosophies to help avoid mistakes and pitfalls and build a simple and robust task ecosystem.

We have prepared a number of opinionated tools that you can use to build your task ecosystem quickly and robustly. We 
encourage you to head on over to github.com/pcelvng/task-tools to see what tools are available to solve common task ecosystem
problems.