# Hello World! Taskmaster

Get up and going with your first taskmaster.

First, build the hello-world taskmaster application.

```bash
$ go build
```

Run the application and see the worker in action.

```bash
$ ./taskmaster -type='hello-world' -info='Hello World!'
```

The screen output should be very similar to (different created dates):

```bash
{"type":"hello-world","info":"Hello World!","created":"2017-01-01T00:00:01Z"}
```

The output was sent to the stdout task bus. You could redirect the output to 
a file if you wanted:

```bash
$ ./taskmaster -type='hello-world' -info='Hello World!' > tsk.json
```

Look at the contents of 'main.go'. Notice the only requirement for an 
application to qualify as a taskmaster is to produce a task. It's easy enough
to write to stdout but reading and writing to other kinds of task buses can
be a little tricky. That's why task provides several task bus adapters 
out-of-the-box so you don't need to focus on the details of reading and writing
to a task bus.

