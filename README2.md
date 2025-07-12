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
  <a href="https://github.com/mtalijanac/associations.git">
    <img src="images/logo.png" alt="Logo" width="80" height="80">
  </a>

  <h3 align="center">Associations</h3>

  <p align="center">
    Utilities for memory efficient programming.
    <br />
    <a href="https://github.com/mtalijanac/associations"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/mtalijanac/associations/blob/main/src/test/java/mt/fireworks/associations/examples/">View Examples</a>
    ·
    <a href="https://github.com/mtalijanac/associations/issues">Report Bug</a>
    ·
    <a href="https://github.com/mtalijanac/associations/issues">Request Feature</a>
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#Implementation">Implementation</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

Associations is a Java library of collections and utility classes designed to 
handle large amounts of data with minimal memory overhead.

All structures in Associations are:

    * dense - as memory efficient as reasonably possible
    * heap allocated - user friendly design 
    * associative - grouped by matching criteria


Main classes are:

    * CompactMap2 - map of objects
    * BytesMap    - multikey-multimap of objects
    * BytesCache  - BytesMap with additional timing logic
    * ByteList    - dense packed list of byte arrays
    * InternTrie  - intern pool implementation


Associations library was developed as part of large card fraud detection system.
System requirements were transaction processing with milliseconds response,
and keeping track of large data sets used in fraud detection. 


Through years of running system, design has evolved into following choices:

  * densely packed serialized store because it has the lowest memory overhead.
    Data which would take hundreds of gigabytes in POJO form is compressed 
    to just few in its packed representation.

  * heap-allocated structures because they are well-supported by Java tooling
    and marshalling libraries
    
  * rich data indexing. It is important in business settings, where data often
    needs to be retrieved through multiple access paths

  * time-aware data structures, as timestamps frequently serve as the primary
    retrieval criterion
  
  * a garbage collection-aware design. We intentionally did not pursue a 
    garbage-free approach, as it restricts Java programs to a limited subset 
    of libraries that avoid heap allocation. However, the main value of 
    programming in Java lies in its rich ecosystem. 
    

In short, library should be easy to use. It pursues memory efficency.
It is designed for low ms responses, thus it allows for GC to run, but
it will try to avoid memory allocation unless necessary or gc-free path
would be incompatible with "easy to use" part.


## Classes

### InternTrie

InternTrie is inter pool. Just as String intern pool, it is inteded to remove
duplicated objects from heap but it works with any object type. Where InternTrie 
really differs from String intern pool is that it inters object based on 
their marshalled bytes.

Thus we can use arbitary byte array, ask InternTrie "hey do you know of this object",
and it will returned pool object if such exists. Used well, InternTries removes 
need for unmarshalling any data of low cardinality. 

Example:

    bytes[] goodBytes_1 = "GOOD".getBytes(UTF-8);
    bytes[] goodBytes_2 = "GOOD".getBytes(UTF-8);
    
    InternTrie<String> it = new InternTrie<String>();
    String good_1 = it.intern(goodBytes_1, bytes -> new String(bytes, UTF-8));
    String good_2 = it.intern(goodBytes_2, bytes -> new String(bytes, UTF-8));
    
    if (good_1 == good_2)
      System.out.println("It works!!");
  
This example will print 'It works!!", as two distinct byte arrays are "unmarshalled"
into the same string object. String is unmarshalled only once, on the first occurence
of byte array. On second intern call, content of goodBytes_2 array is used as key
to find proper interned string.

Thus InternTrie should be only used with objects which have low cardinality.
Like common string used for flag etc. In business enviroment that kind of data
is actually very common.

See [Value of Intern](https://github.com/mtalijanac/articles/blob/main/ValueOfInter.md)
for longer explanation of motivation and usage of InternTrie.


### CompactMap2

It's a poor man map with a one valuable proposition - it is extremely well packed
and thus very memory efficient.

This map stores object in marshalled form - as bytes into larger byte array pools.
Thus during construction proper implementation of SerDes<T> has to be passed
to map constructor.

Second constructor argument is Function<T, byte[]> keyer method. Keyer is function
which returns bytes key for a given object. This key is used as object key within
map. Thus instead of having put method

    // java map put and get
    T oldObject = map.put(key, object)
    T storedObject = map.get(key);

CompactMap2 has add method:

	// CompactMap2 add and get
	byte[] key = map.add(object);
	T object = map.get(key);

Adding objects with same key doesn't remove old objects from map. They become
unreachable but their [*memory remains*](https://www.youtube.com/watch?v=RDN4awrpPQQ)..
In order to clean map of this dead objects, map has to be "compacted" by invoking
compact method which is a form of poor-man's gc. Compact map is perfectly usable
during compaction, but it is cpu and memory intensive operation so it is perfectly
reasonable to assume that compacting CompactMap will trigger Java's GC making it 
doubly expesive.

Good strategy to using CompactMap are either: 

    * don't overdo it - don't grow unconstrained, don't use it for
      values which die frequently
     
    * have a 'loading level' phase - occasionally when system allows
      call compact method to release dead memory
    

See [usage example](https://github.com/mtalijanac/associations/blob/main/src/test/java/mt/fireworks/associations/examples/CompactMap2_UsageExample1.java)

** Comparison to Caffeine (Guava Cache)
TBD



<!-- LICENSE -->
## License

Distributed under the GPL3 License. See `LICENSE` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

Marko Talijanac - marko.talijanac@remove.gmail.com

Project Link: [https://github.com/mtalijanac/associations](https://github.com/mtalijanac/associations)

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

All credit to Best-README-Template for being just awesome!!

* [Best-README-Template](https://github.com/othneildrew/Best-README-Template)

<p align="right">(<a href="#readme-top">back to top</a>)</p>



