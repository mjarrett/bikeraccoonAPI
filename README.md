# bikeraccoonAPI

This is the source code for both the GBFS tracking app and API server hosted at [api.raccoon.bike](https://api.raccoon.bike)

## GBFS Tracker

Many bike share systems worldwide provide real-time access to system information via the GBFS specification. For more information about GBFS, see [the GBFS github repo](https://github.com/NABSA/gbfs). GBFS provides the number of bikes and docks at each station and the number of available floating bikes, but it does *not* provide trip information. By monitoring the number of available bikes in a system and tracking changes over time this tool estimates the number of trips in a system each hour. For a more detailed explanation of how a previous implementation of this tool works, see [this blog post](https://notes.mikejarrett.ca/tracking-bikeshare-use-using-gbfs-feeds/). 

It's important to keep in mind that the trip counts infered by this tracker are estimates only and may vary substantially from official counts.

## Bike Raccoon API

To access the data collected by the tracker, we provide the following HTTP endpoints. All endpoints return JSON text.

* **systems**

  Returns a list of systems being tracked along with some system metadata.

  *Example*: [https://api.raccoon.bike/systems](https://api.raccoon.bike/systems)

* **stations**

  Returns a list of stations for a given system along with station metadata.
  
  *Parameters*: 
    system: the system name (as specified in the systems endpoint)
    
  *Example*: [https://api.raccoon.bike/stations?system=bike_share_toronto](https://api.raccoon.bike/stations?system=bike_share_toronto)
    

* **activity**
  
  Returns trip activity data
  
  *Parameters*:
    system: the system name (as specified in the systems endpoint)
    start: The starting datetime, format: YYYYMMDDHH
    end: The ending datetime (inclusive), format: YYYYMMDDHH
    frequency: The period in which to group the data. Options are 'h' (hours, default), 'd' (day), 'm' (month), 'y' (year).
    station: The station ID of the station (as specified in the stations endpoint). If no station is provided (default), data for the whole system will be returned. If 'all' is provided, data for each station in the system will be returned. If 'free_bikes' is provided, data for free floating bikes is returned.
    
  *Example*: [https://api.raccoon.bike/activity?system=mobi_vancouver&start=2021012800&end=2021012900&frequency=m&station=0001](https://api.raccoon.bike/activity?system=mobi_vancouver&start=2021012800&end=2021012900&frequency=m&station=0001)  
  
  
# License

This software is licensed under the [MIT license](https://opensource.org/licenses/MIT).

Data provided by the BikeRaccoonAPI is licensed under the [Creative Commons BY 4.0 license](https://creativecommons.org/licenses/by/4.0/). 
You are free to:

* Share — copy and redistribute the material in any medium or format
* Adapt — remix, transform, and build upon the material
for any purpose, even commercially.

You must give appropriate credit, provide a link to the license, and indicate if changes were made. You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
You may not apply legal terms or technological measures that legally restrict others from doing anything the license permits.
