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

