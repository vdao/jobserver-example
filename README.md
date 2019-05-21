# Section 1

### Parsign data


```scala
val df = new Parser(spark).parse("test-data/events.txt")
```


event_type|                  id|       impression_id|movie|            movies|user_id
----------|--------------------|--------------------|-----|------------------|-------
impression|67d38186-bb30-463...|                null| null|\[1, 2, 3, 4, 5, 6\]|      1
impression|a759596a-ae4b-461...|                null| null|\[1, 2, 3, 4, 5, 6\]|      2
impression|46b4d266-eb19-4d9...|                null| null|\[1, 2, 3, 4, 5, 6\]|      3
impression|5ada269e-d1ef-47b...|                null| null|\[1, 2, 3, 4, 5, 6\]|      4
impression|3a8818a1-947f-48e...|                null| null|\[1, 2, 3, 4, 5, 6\]|      5
impression|fae7d5ff-118a-4e3...|                null| null|\[1, 2, 3, 4, 5, 6\]|      6
impression|30f5b38e-ff00-4eb...|                null| null|\[1, 2, 3, 4, 5, 6\]|      7
impression|bad942ba-991b-4de...|                null| null|\[1, 2, 3, 4, 5, 6\]|      8
impression|c60cca7a-ba23-4b8...|                null| null|\[1, 2, 3, 4, 5, 6\]|      9
impression|2e45d4e8-79ba-4e2...|                null| null|\[1, 2, 3, 4, 5, 6\]|     10
impression|d955d25c-00f6-497...|                null| null|\[1, 2, 3, 4, 5, 6\]|     11
impression|91095659-73c6-4d9...|                null| null|\[1, 2, 3, 4, 5, 6\]|     12
impression|c6dd60aa-5f2f-497...|                null| null|\[1, 2, 3, 4, 5, 6\]|     13
impression|f8e9abf5-23b4-43e...|                null| null|\[1, 2, 3, 4, 5, 6\]|     14
impression|2f91fd16-6318-4d8...|                null| null|\[1, 2, 3, 4, 5, 6\]|     15
impression|cd1f2e9f-8e2b-42f...|                null| null|\[1, 2, 3, 4, 5, 6\]|     16
conversion|                null|3a8818a1-947f-48e...|    1|              null|   null
conversion|                null|3a8818a1-947f-48e...|    2|              null|   null
conversion|                null|3a8818a1-947f-48e...|    3|              null|   null
conversion|                null|c6dd60aa-5f2f-497...|    1|              null|   null
conversion|                null|c6dd60aa-5f2f-497...|    2|              null|   null
conversion|                null|c6dd60aa-5f2f-497...|    4|              null|   null
conversion|                null|f8e9abf5-23b4-43e...|    3|              null|   null
conversion|                null|f8e9abf5-23b4-43e...|    4|              null|   null
conversion|                null|f8e9abf5-23b4-43e...|    6|              null|   null
conversion|                null|bad942ba-991b-4de...|    4|              null|   null
conversion|                null|bad942ba-991b-4de...|    5|              null|   null
conversion|                null|bad942ba-991b-4de...|    6|              null|   null


