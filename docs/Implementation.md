<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a name="readme-top"></a>
<!--
*** Thanks for checking out the Best-README-Template. If you have a suggestion
*** that would make this better, please fork the repo and create a pull request
*** or simply open an issue with the tag "enhancement".
*** Don't forget to give the project a star!
*** Thanks again! Now go create something AMAZING! :D
-->


<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/mtalijanac/timecache.git">
    <img src="../images/logo.png" alt="Logo" width="80" height="80">
  </a>

  <h3 align="center">Implementation</h3>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#keyer">Keyer</a></li>
    <li><a href="#bytelist">ByteList</a></li>
    <li><a href="#index">Index</a></li>
    <li><a href="#storage">Storage</a></li>
    <li><a href="#serdes">SerDes</a></li>
  </ol>
</details>



<!-- KEYER -->
## Keyer

Keyer is java Function which associates data to a unique key.


**Keyer must be idempotent!!!**.

TimeCache dosent use keys to store data, instead it uses keyer function
to extract a key from data and than associatess data with calculated key.

Main difference to a normal map is that data is fetch by data:

    Timecache cache = ...
    cache.add(data);

    List<Entry<byte[], List<T>>> = cache.get(data);

Normal java.util.Map uses put metod to associate key with value:

    HashMap map = new HashMap();
    map.put("key", "firstValue");
    map.put("key", "secondValue");
    String value = map.get("key");

Printin value will output:

    secondValue

Mutlimap uses put method to associate value to a key, but allows
multimple values to be stored under same key.

    Multimap<String, String> map = ArrayListMultimap.create();
    map.put(key, "firstValue");
    map.put(key, "secondValue");
    Collection<String> values = map.get(key);

Printing values will print:

    [firstValue, secondValue]

Byte array should allways be constructable fro



<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- BYTELIST -->
## ByteList

ByteList is list of bytes, optimized for storing byte arrays.
Timecache uses ByteList as to store of objects in their serialized form.

There is only one way to add data to to a bytelist - by using add method.
There are however many ways to access and/or read data from ByteList.
There is no way to remove data from ByteList.

Simple usage example:

    byte[] dataToStore = ....
    long key = byteList.add( dataToStore );
    byte[] readData = byteList.get( key );

Important thing to notice is that returned key is of **long** type.
So actual maximum size of list is enormous. For richer examples see [usageExample](https://github.com/mtalijanac/timecache/blob/main/src/test/java/mt/fireworks/timecache/ByteListTest.java).

Under the hood ByteList is implemented as ArrayList of byte arrays, called buckets.
When data is added to list, first length of data is written to a free position in
latest bucket. Then data is copied to following position. So data is stored
with lenght prefix. Size of prefix / data header is 2 bytes. Returned key is
essentially index to a data header. When reading data, key is decomposed to
index of bucket in the ArrayList, and to an offset in the bucket fetched.

Default configuration:
  - each bucket is 1 mb of size
  - data header is 2 bytes

Because 2 byte header, maximum data array written to byteList cannot exceed 64kb.
With default configuration storage efficiency is quite high. Essentially for each
entry added, 2 bytes are lost to a header. When data + dataHeader is too large to
fit at end of bucket, a new bucket is allocated and data is written to a start
of new bucket. Free space at end of previous bucket is "lost".

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- STORAGE -->
## Storage

Storage is collection of ByteLists. Each ByteList in storage is associated
with start and end timestamp, and thus is a time 'Window'. TimeCache uses
Storage to manage lifecycle of events. After all it is a **Time**Cache.

Simple usage example:

    byte[] dataToStore = ...
    long tstamp = System.currentTimeMillis();
    long key = storage.addEntry(tstamp, dataToStore);
    byte[] dataRead = storage.getEntry(key);

Essentially, storage will store data to a underlying ByteList. It uses passed
timestamp to choose to a which of many ByteLists will it store this data.
Returned key encodes timestamp, thus to which ByteList is data stored,
and index within that ByteList.

Storage key encoding is fundamental to a efficient TimeCache implementation.
First 29 bits of key encodes offset in *seconds* from TimeCache *epoch*.
TimeCache epoch is starting second of year in which TimeCache instance was
**created**. 29 bits is enough to store more than 17 years of seconds.
Following 35 bits are used to store **index** in ByteList.
While ByteList is perfectly capable of storing 2^63 bytes of data, because
storage key design, only lower 35 bits are used for maximum of 32gb of
data within one window.

Because data timestamp is, at least partly to a second resolution, encoded within
key some basic equals comparison of data is possible without ever fetching data.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- INDEX -->
## Index

Index is multimap of data key to stora key.
Data key is result of keyer on data

T.B.D.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- SERDES -->
## SerDes

T.B.D.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



