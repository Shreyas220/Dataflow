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

GOAL
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