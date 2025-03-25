Current 
```
                                              ┌─────────────┐
                                              │             │
┌─────────┐                                   │             │
│         │               ┌─────────────┐     │             │
│  source │─────────--───▶│ BatcherNode │────▶│  Persistent │
│         │               └─────────────┘     │    store    │
└─────────┘                                   │             │
                                              │             │
                                              └─────────────┘
```
## Goal
Goal of this project to implement certain functioanlities defined by the Dataflow paper (foundation for apache Beam) to build an iceberg writer/persis

## Functioanlity (ToDo)
- Time based windows (adjustable)
- Window rotation
- Processing the window based on partitioned Values
- Handling CDC data (sub paritionining the batches based on cdc timestamp to maintain order)
- Backpressure
- Adjusts batch sizes based on processing performance

## Benchmark 

Where sample data looked like 
`{"name": "A", "size": "small", "count": 2}`

Total number of events in 1 batch = 10,000

partitioned on `count` and randomness was limit to 10 in data gen 

```
Arrow conversion took: 170.850ms
Partitioning took: 34.328ms
Partitioned length: 10
```


Ideally 
```

                          ┌─────────────┐     ┌─────────────┐
                     ┌───▶│ BatcherNode │────▶│             │
┌─────────┐          │    └─────────────┘     │             │
│         │          │    ┌─────────────┐     │             │
│  source │─────────▶│───▶│ BatcherNode │────▶│  Persistent │
│         │          │    └─────────────┘     │    store    │
└─────────┘          │    ┌─────────────┐     │             │
                     └───▶│ BatcherNode │────▶│             │
                          └─────────────┘     └─────────────┘

```

### Ref
dataflow paper https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/43864.pdf