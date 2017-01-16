# HadoopKMeansClustering

Hierarchical K-means nD algorithm using Hadoop MapReduce.

## Usage : 

```
hadoop jar kmeans-0.0.1.jar [inputFile] [outputDirectory] [numberOfClusters] [maxDepth] [labelColumn] [mesureColumn] [dimensionsColumns..]
```

* inputFile - Path to the input file
* outputDirectory - Path to the output file
* numberOfClusters - The number of clusters 
* maxDepth - Maximum depth of the hierarchy
* labelColumn - The column number to use as label
* mesureColumn - The column number to use as mesure (for labels)
* dimensionsColumns - The columns to use as dimensions

## Exemple : 

### Input file :

[See the full input](Input_sample) 

#### sample.csv (only the first 10 lines) :

|             |           |            |            |             | 
|-------------|-----------|------------|------------|-------------| 
| John        | Carpenter | 137.08665  | 76.83012   | 250.02499   | 
| Douglas     | Brooks    | 756.16506  | 1998.86466 | 109.98914   | 
| Christopher | Ellis     | 1440.21484 | 1613.78122 | 1791.97208  | 
| Ronald      | Day       | 1681.68424 | 545.76823  | 1518.54743  | 
| Adam        | Franklin  | 355.17699  | 1627.90288 | 805.90645   | 
| Martha      | Wagner    | 1490.26187 | 1130.03765 | 1535.13642  | 
| Lois        | Knight    | 622.03223  | 168.97468  | 782.15226   | 
| Diane       | Ray       | 1434.58917 | 1301.34475 | 250.6868    | 
| Carlos      | Foster    | 788.38278  | 1806.00689 | 1206.03318  | 
| Susan       | Thomas    | 707.39161  | 1052.23442 | 1332.81622  | 


> **Note:**

> - File must be a csv file with comma as separator
> - The column to use as mesure and the columns to use as dimensions must contains numbers

### Command :

```
hadoop jar kmeans-0.0.1.jar dir/input/sample.xml dir/output/ 3 3 1 2 2 3
```

### Output files :

[See the full output](Output_sample) 

#### result.csv (only the first 10 lines)  :

|        |       |       |           |            |            |            |             | 
|--------|-------|-------|-----------|------------|------------|------------|-------------| 
| 0     | 0     | 0     | Antonio   | Clark      | 1053.39842 | 1017.81721 | 476.47411  | 
| 0     | 0     | 0     | Scott     | Kim        | 1292.48639 | 1148.74827 | 1675.41528 | 
| 0     | 0     | 0     | Katherine | Watson     | 1331.90275 | 916.70113  | 854.72504  | 
| 0     | 0     | 0     | David     | Morris     | 1029.97903 | 1126.23399 | 217.69392  | 
| 0     | 0     | 0     | Joshua    | Alvarez    | 1157.80344 | 1099.08663 | 253.90982  | 
| 0     | 0     | 0     | Sean      | Wells      | 1034.09352 | 1211.43038 | 1503.6274  | 
| 0     | 0     | 0     | Kathy     | Wright     | 1283.8311  | 1220.29759 | 1994.45327 | 
| 0     | 0     | 0     | Sandra    | Ortiz      | 1218.60772 | 1266.37722 | 176.4639   | 
| 0     | 0     | 0     | Emily     | Shaw       | 1144.66323 | 1231.03166 | 177.44444  | 
| 0     | 0     | 0     | Janet     | Richardson | 1138.32828 | 1062.63822 | 276.52458  | 
 

#### label.0.csv :
|       |         |            | 
|-------|---------|------------| 
| 0     | Thomas  | 1997.85227 | 
| 1     | Douglas | 1026.07475 | 
| 2     | Cynthia | 1996.04895 | 


#### label.1.csv :
|       |       |         |            | 
|-------|-------|---------|------------| 
| 0     | 0     | Ashley  | 1626.00058 | 
| 0     | 1     | Thomas  | 1997.85227 | 
| 0     | 2     | Roy     | 1996.59027 | 
| 1     | 0     | Douglas | 1026.07475 | 
| 1     | 1     | Douglas | 756.16506  | 
| 1     | 2     | Carol   | 761.75101  | 
| 2     | 0     | Theresa | 1443.2877  | 
| 2     | 1     | Cynthia | 1996.04895 | 
| 2     | 2     | Roy     | 816.74338  | 

#### label.2.csv :
|       |       |       |           |            | 
|-------|-------|-------|-----------|------------| 
| 0     | 0     | 0     | Andrew    | 1356.10306 | 
| 0     | 0     | 1     | Kevin     | 1375.79162 | 
| 0     | 0     | 2     | Ashley    | 1626.00058 | 
| 0     | 1     | 0     | Thomas    | 1997.85227 | 
| 0     | 1     | 1     | James     | 1527.76325 | 
| 0     | 1     | 2     | Paula     | 1985.59547 | 
| 0     | 2     | 0     | Anne      | 1981.78594 | 
| 0     | 2     | 1     | Roy       | 1996.59027 | 
| 0     | 2     | 2     | Irene     | 1760.69654 | 
| 1     | 0     | 0     | Stephanie | 745.42596  | 
| 1     | 0     | 1     | Douglas   | 1026.07475 | 
| 1     | 0     | 2     | Doris     | 1008.43379 | 
| 1     | 1     | 0     | Evelyn    | 429.62616  | 
| 1     | 1     | 1     | Douglas   | 756.16506  | 
| 1     | 1     | 2     | Katherine | 312.69764  | 
| 1     | 2     | 0     | Linda     | 471.94682  | 
| 1     | 2     | 1     | Steven    | 447.08353  | 
| 1     | 2     | 2     | Carol     | 761.75101  | 
| 2     | 0     | 0     | Brenda    | 1024.33446 | 
| 2     | 0     | 1     | Lori      | 1232.19069 | 
| 2     | 0     | 2     | Theresa   | 1443.2877  | 
| 2     | 1     | 0     | Harold    | 1743.58156 | 
| 2     | 1     | 1     | Cynthia   | 1996.04895 | 
| 2     | 1     | 2     | Melissa   | 1676.56816 | 
| 2     | 2     | 0     | Arthur    | 687.44458  | 
| 2     | 2     | 1     | Roy       | 816.74338  | 
| 2     | 2     | 2     | Joseph    | 343.96899  | 

## Licence :
```
MIT License

Copyright (c) 2016 Valentin Colmant

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
