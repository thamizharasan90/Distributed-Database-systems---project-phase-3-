# Phase III 

## Preprocessing data

- Read CSV into HDFS
- Remove Header
- Remove unnecessary columns, take necessary ones
- Remove points outside envelope -> 40.5–40.9, -74.25 — —73.7

## Computing Cells

* Compute max, min longitude and latitude

* Form a grid —>  M *N dimensions where :

  * M — ((maxLongitude-minLongitude) * 100 )+2

  * N — ((maxLatitude-minLatitude) * 100 )+2

    2 is a buffer to include border points

* Make 2d integer array from M*N —> zip it as an 1-d array by row-wise major order

* Keeping (minLongitude,minLatitude) as (0,0) transform all points on the plane

* Define function isInCell to compare (x,y) to any Cell (i,j) as :

  * $ if\ i \le x*100\ \le i+1 \\ \ \ \ \ and j\le y*100\le j+1  $

  **Example computation**

## Computing neighborhood of a cell

* Corners, Gutters and cells —> 12, 18 and 27 neighbors respectively