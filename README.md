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

  <h3 align="center">Timecache</h3>

  <p align="center">
    An awesome MultiMap for caching events!
    <br />
    <a href="https://github.com/mtalijanac/associations"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/mtalijanac/associations/blob/main/src/test/java/mt/fireworks/associations/examples/UseAsMutlimap.java">View Demo</a>
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

TimeCache is data structure used to store related events with expiry.
TimeCache is a form of multimap, as it stores multiple entries under the same key.
Additionally each stored value can be associated to multiple keys.
TimeCache is a cache, as data old data will eventually be cleared.
TimeCache is primary optimized for memory footprint. Each value is
serialized and then stored to an on-heap allocated array.

Here's when you should use TimeCache:
* You need to store large amount of data, in space efficient manner
* Data stored is associated to other stored data (grouping data by index)
* Data stored is associated in multiple ways (arbitrary number of indexes)
* Data is removed periodically based on its age (windowing)

<p align="right">(<a href="#readme-top">back to top</a>)</p>




<!-- USAGE EXAMPLES -->
## Usage

Test folder contains many [examples](https://github.com/mtalijanac/associations/blob/main/src/test/java/mt/fireworks/associations/examples)
of usage.

Most important:
 - using Timecache as [multimap](https://github.com/mtalijanac/associations/blob/main/src/test/java/mt/fireworks/associations/examples/UseAsMultimap.java)
 - passing Time and [handling time windows](https://github.com/mtalijanac/associations/blob/main/src/test/java/mt/fireworks/associations/examples/WindowHandling.java)


<!-- IMPLEMENTATION -->
## Implementation

Under the hood TimeCache is bunch of Java 8 wrappers for byte arrays and
some Eclipse Collections magic.

Refer do [implementation docs](https://github.com/mtalijanac/associations/blob/main/docs/Implementation.md)
for deeper understanding of individual components.

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



