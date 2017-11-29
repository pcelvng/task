# Backloader Taskmaster

Used for creating one or more tasks of a particular type
for backloading over a period of time.

simplest usage:

```bash
go build
./backloader -t=task-type 
```

common usage:

```bash
./backloader -t="task-type" -template="{yyyy}-{mm}-{dd}T{hh}:00" -from="2017-01-01T00" -to="2017-02-01T00"
```

Note: the 'to' date is inclusive

To see all options do:

```bash
./backloader --help
```
