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
    <img src="images/logo.png" alt="Logo" width="80" height="80">
  </a>

  <h3 align="center">Timecache</h3>

  <p align="center">
    An awesome MultiMap for caching events!
    <br />
    <a href="https://github.com/mtalijanac/timecache"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/mtalijanac/timecache/blob/main/src/test/java/mt/fireworks/timecache/examples/UseAsMutlimap.java">View Demo</a>
    ·
    <a href="https://github.com/mtalijanac/timecache/issues">Report Bug</a>
    ·
    <a href="https://github.com/mtalijanac/timecache/issues">Request Feature</a>
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#bytelist">ByteList</a></li>
    <li><a href="#index">Index</a></li>
    <li><a href="#storage">Storage</a></li>
    <li><a href="#serdes">SerDes</a></li>
    <li><a href="#keyer">Keyer</a></li>
  </ol>
</details>



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

Important thing to notice is that key of ByteList is of **long** type.
So actual maximum size of list is enormous. For richer examples see [usageExample](https://github.com/mtalijanac/timecache/blob/main/src/test/java/mt/fireworks/timecache/ByteListTest.java).

Under the hood ByteList is implemented as ArrayList of byte arrays, called buckets.
When data is added to list, first length of data is written to a latest bucket,
and then data is copied to bucket. Returned key is essentially index to a data.
When reading data, key is decomposed to index of bucket in arrayList, and to an
offset in that bucket.

Default values:
  - each bucket is 1 mb of size
  - data header is 2 bytes

Because 2 byte header, maximum data array written to byteList cannot exceed 64kb.
Storage efficiency is quite high. Essentially for each entry added, 2 bytes are
lost to a header. When data + dataHeader is too large to fit at end of bucket,
a new bucket is allocated and data is written to a start of new bucket.
Free space at end of previous bucket is "lost".

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- INDEX -->
## Index

T.B.D.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- STORAGE -->
## Storage

T.B.D.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- SERDES -->
## SerDes

T.B.D.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- KEYER -->
## Keyer

T.B.D.

<p align="right">(<a href="#readme-top">back to top</a>)</p>
