# Bus

Implemented buses are a little more than a simple integration with a bus technology.

Namely, a bus implementation should make sure:

- reading a task is lazy loaded
- lazy loading produces no side effects

## Lazy Loading

Many messaging technologies want high throughput and will greedily retrieve the next message from a topic. 
Task bus readers need to make sure that only a single task message is retrieved at a time, 
even if this means disconnecting and reconnecting from the messaging technology each time a 
new task is requested. Remember that in batch processing the bottleneck is not fetching
in the next task but rather completing the task itself. For high throughput tasks it is 
recommended to use multiple instances of the worker.

## Test Cases

- Consumer messages are lazy loaded
- Stop() can be called even when multiple calls are waiting for tasks
- Stop() is non-blocking (even with outstanding Msg() or Send() requests)
- Stop() is safe to call multiple times
- Msg() is safe to call even after calling Stop()
- Stop() is safe to call even if Msg() or Send() have not been called yet
- Once consumer returns done=true then subsequent calls to Msg() should return done=true.