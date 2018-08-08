# stream-readers

Why Actors?    
1. More fun, safe & reactive.
2. The idea is actors are cheaper than a threads.  

Simple low-level way to solve this task with low-level concurrency could be  
[this one](https://www.quora.com/How-can-I-listen-to-two-or-more-InputStreams-concurrently-in-Scala-or-Java)  
Yet I don't like low-level concurrency.  

This solution doesn't have timeout input from console & error handling, since I was more interested in how to model this problem using Akka, instead of low-level concurrency).    