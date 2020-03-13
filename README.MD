# Phase 3 Method Explanations

## Add Plane

Simply asks user for:
- Plane ID
- Plane Model
- Plane Age
- Total seat numbers

Then Updates Plane table with the new content.

## Add Pilot

Asks user for:
- Pilot ID
- Pilot's fullname
- Pilot's nationality

Updates plane table with new content

## Add Flight

Asks user for:
- Flight num
- Number sold
- Stops
- Departure date
- Arrival date
- Departure and arrival airport
- Update Flight table with info

Then we need to update FlightInfo table
Asks user for:
- Flight info ID
- Re-uses the flight number from above
- Plane ID
- Pilot ID

Completed query to update FlightInfo table

## Add Technician
Ask user for:
- Technician ID
- Technician fullname

Updates technician table

## List number of available seats
Asks user for:
- Flight num
- Flight year
- Flight date

In the query we have to receive info from Flight, Plane, and FlightInfo
1. Find total seats from the plane and subtract from number sold
2. Show the actual departure date of the flight
3. The query matches plane id's between plane and flightinfo to find the correct plane
4. Match the actual departure date with the user's requested flight date.
5. Match flight number with the user's reuqested flight num
6. Group by seats available and departure date.

## List total number of repairs per year

Immediately runs query to grab all repairs for each year
1. Organizes the date by first grabbing the year from the date
2. Counts the number of repairs within that year
3. Groups by year in ascending order
