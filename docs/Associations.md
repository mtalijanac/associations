
Associations
-------------

'Associations' project is collection of data structures for storing "associated" data.
Assoicated data is, essentially, data stored under a same key, something for what we
usually use multimaps. Most common multimap implementation in Java is probably Guava's
implementation:

    Multimap<String, String> toys = ArrayListMultimap.create();

    toys.put("Ana", "doll");
    toys.put("Ana", "house");
    toys.put("Bob", "car");
    toys.put("Bob", "house");

    Collection<String> toysForBob = toys.get("Bob"); // ["car", "house"]
    Collection<String> toysForAna = toys.get("Ana"); // ["doll", "house"]

toysForBob are toys associated with Bob, and toysForAna are Ana's.

Similar functionality is provided here by BytesMap, but with few caveats.
Major difference being, that associations in multimap are arbitary. They are set
at a moment of putting a key and a value into map. Meanwhile, in bytes map key is
calculated from value being stored.


    BytesMap<String> map = BytesMap.newInstance(String.class)
        .withSerdes(Associations.stringSerDes())
        .associate("toys", (String val) -> val.split(";")[0].getBytes(UTF_8))
        .build();

    map.add("Ana;doll");
    map.add("Ana;house");
    map.add("Bob;house");








For example 'ownership of toys' in kindergarter could be implemented as multimap:

   MultiMap<String /*name*/, List< String /*toy*/>> ownership = ......
   map.add("


In computing, we don't usually reffer to data as "associated".
Proper term is "correlated".



For example of associated data think about ledger tables.
Common Structure


  Date  |  Description | Account nb. | DR or CR | Debit $ | Credit $
--------------------------------------------------------------------
1/1/203 | D