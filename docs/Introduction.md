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
    <img src="../images/logo.png" alt="Logo" width="80" height="80">
  </a>

  <h3 align="center">Introduction</h3>
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


## Event

In computing an **Event** is data with a time.
Minimal possible amount of data is "name".
Minimal possible amount of time is "when did it happened".
*Minimal event* is thus **NAME with TIMESTAMP**:

  - "Restart" at midnight
  - "Shootout" at high noon
  - "Birthday" at September 26, 1979, 5 min after midnight

Events are used to tell a story. And lot can be said by using even simplest one.
At end of the day each election victor is story told with a one event:
one name and one timestamp.

And while short stories can be powerfull [For Sale: baby shoes, never worn]
or tragic [account ballance, today], other require more work:

  - Debit card "Payment", 90€, in Zara, Madrid, yesterday at 14h

This story has more data to its event than just a name:

  - name: "Payment"
  - method: "debit card"
  - amount: "90"
  - currency: €
  - merchant: Zara
  - location: Madrid

Adding data to event is one way to tell more complex story.
It isn't the only way. Adding more time works too:

  - "Fire", started at 13:31, ended at 15:40

Fires have **durations** - pairs of start and end timestamps.
Events often have durations. And more than one too:

  - "House fire", fire started 13:31,
                  fire alarm on 13:32,
                  firemen notified at 13:34,
                  firemen responed at 13:56,
                  fire alarm off 13:58,
                  fire extinguished 15:40

House, alarm, firemen - each of them has its own view of event,
own timeline of observing and reacting to it; and thus similar, but
not quite same story to tell about same fire.

That is because events do not exist in vacuum. They have **interested parties**.
When events are shared by multiple parties they have multiple
durations and all sorts of interesting correlations.

If you think abut it, at some level of abstraction, each episode of Poirot
is essentially retelling of same event, from multiple **observers**.


Finally, great stories are made by using sequences of
events. And not sequence of any random events (a dog barks, a bee polinates, a null pointer),
but one where events have **causality**. Causal events are the ones
that:

  - are associated - other word for sharing parts of data
  - have proper time ordering

Timecache is data structure for storing time ordered associated events.

<p align="right">(<a href="#readme-top">back to top</a>)</p>
