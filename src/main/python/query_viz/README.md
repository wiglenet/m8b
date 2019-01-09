# Visualizer for Magic (8) Ball results
This is a simple test project for visualizing the corresponding m8b cells for a `scan` result in m8b on Google Earth™ or other KML viewers.

### Requirements
Python 3. A virtual environment is strongly recommended.

### Installation
Includes a requirements.txt file for `pip` installation. In your installation directory:

```
pip install -r requirements.txt
```


### Usage:
This python program accepts and output stream from the m8b commandline `query` command - inline it in your favorite shell.

```
java -jar ../../../../target/m8b-1.0-SNAPSHOT.jar query <path to your m8b file> query <BSSID list> | python m8b_query_viz.py  > ouput.kml
```
then just open `output.kml` with your favorite viewer.

_Note_ if you elect to use the WiGLE-released m8b file, we strongly recommend enabling thresholding (comment line 96, uncomment line 95), since nothing available today can visualize all the low-probability quads.

