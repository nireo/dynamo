# dynamo


## Notes

* Store objects that are relatively small (less than 1 MB)
* ACID leads to poor availability
* Dynamo makes the trade-off of having weaker consistency to have better availability.
* No isolation
* No locks that improves performance
* Has to be very efficient (focus on latency and throughoutput)
* Dynamo should be running in a non-hostile environment
* Dynamo was built under the assumption that each part of Amazon should have their own dynamo instance. For example checkout has it's own dynamo instance the store page etc. This means that the scalability doesn't need to target thousands of nodes rather it's more targeted at a few hundred nodes.
* It should be easy to add and remove physical nodes
* Every node should have the same responsibility
* Heterogenity aka should be able to have physical nodes with different performance capabilities and still function well.
