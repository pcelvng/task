# Hello World! Worker

Get up and going with your first worker.

First, build the hello-world worker application.

```bash
$ go build
```

Run the application and see the worker in action.

```bash
$ cat tsk.json | ./worker > done.json 
```

The screen output should be very similar to:

```bash
2017/01/01 00:00:05 Hello World!
```

That output represents what the worker *did*. In this case the worker
just logged the value of "info" from the task.

Now look at the contents of 'done.json'. It should look very similar to:

```bash
{"type":"hello-world","info":"Hello World!","created":"2017-01-01T00:00:01Z","result":"complete","msg":"task complete!","started":"2017-01-01T00:00:05Z","ended":"2017-01-01T00:00:06Z"}
```

Finally, look at the contents of 'tsk.json'

```bash
{"type":"hello-world","info":"Hello World!","created":"2017-01-01T00:00:01Z"}
```

A task is a **signal** that tells a worker **to do something**. The 'info' contents tell 
the worker the details of **what** to do.

Let's look at the pieces of a task object:

**type** 

Identifies the type of work to be done. The 'hello-world' worker knows how to
do tasks with type 'hello-world'. If the task were of a different type the worker application
could choose to reject the task and send it back with an error message. The 
worker application may choose not to reject a task with the wrong type 
but instead try and work on the task anyway. That's ok, the 'type' in its
most basic usage is just a way to communicate 'intent'. That is, the task
is *intended* to be worked on by a worker that knows how to complete a task
of that type. In our basic example we did not enforce only working on tasks
of the right type. The launcher has the ability to handle tasks of the wrong
type by either rejecting the task or discarding it. 

**info**

Tells the worker more information about the specifics of completing the task. 
In this simple worker the info was used as the content of the log message. 

To see it in action go ahead and change the 'info' value in tsk.json and then
run the application again. What output do you see?

**created**

Simply describes when the task was created. This is good record keeping and
you will see it invaluable when understanding and debugging your task
ecosystem.

**result**

The result value is typically either "error" or "complete". Many other 
job systems try to include many kinds of statuses. We feel that many 
statuses is unnecessary. Part of this extra simplicity is made possible
because Task is a **stateless** system. 

**msg**

The result message. This can be anything the worker decides to make it. 

**started**

The datetime that the worker started work on the task.

**ended**

The datetime that the worker ended work on the task. 