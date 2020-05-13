# Magic (8) balls (.m8bs) 
*imprecise answers to uncertain questions.*


## Background
Named after the oracular toy, magic 8-balls are a long-running, previously theoretical experiment we've been working on at WiGLE.
We're frequently asked for a complete geo-location artifact from WiGLE, but there's a catch: the database is huge and constantly being updated. Worse than this, location queries to an online data sources always run the risk of reporting your position-at-the-time to other entities, even if you trust them to take reasonable precautions with security and privacy. 

In the shadowy past, Uhtu posited that there should be a way to create a minimal, offline artifact that can be effectively queried for low-precision location without continually shipping and re-shipping the entire database.

Our design constraints:

* It has to be fast enough to query and efficient enough for mobile, battery-backed computing devices.
* It has to be orders of magnitude smaller than the complete dataset
* It has to be "accurate enough;" the right square kilometer in areas with sufficient WiGLE coverage
* It needs to be usable offline.
* You may want to build an m8b using *any* collection of private data (without WiGLE), the tool must be public, the inputs can be private, and the results must be repeatable.

Today, 17 years after the start of WiGLE, we introduce the m8b: we hope you enjoy it both for what it can do and the idea that it demonstrates. This is provided as-is, as a proof of concept. We know there are a lot of folks out there much smarter than us, and we welcome their criticism, contribution, and refinement of what we think could be a powerful innovation in privacy, safety, and location awareness. Keep on stumbling, and remember: wherever you go, there you are.

## Details
*"Reply hazy, try again"*

The Magic 8 Ball is a parametric data structure and set of functions for
deriving coordinates from a set of identifiers, and optional storage
formats for the same.

The general intent of the magic eight ball (M8B) is the ability to statistically
derive a set of coordinates from a set of identifiers, without storing the 
entirety of the original identifiers.

It can be considered a mapping or one-way function 
    `m8b({identifiers}) -> {{coordinates}}`

Histogramming the resulting coordinates sets will give indications as to the 
coordinates most associated with the set of input identifiers. The practical
application of this is that if you tell an M8B about the various networks you
can see, it can suggest where you are.

### Better not tell you now

The reference set of identifiers used will be 802.11 MAC addresses (6 bytes)
hashed with SipHash-2-4 and a fixed 0 key, the result of which will be masked
off to only use the low-n bits.

The design requirement here counter-intuitively is to ensure evenly distributed
collision; multiple identifiers must share the same hashed result. Querying
the artifact must be able to determine the absence, but not the presence, 
of a given MAC. 


### Concentrate and ask again

The source (lat,lon) tuple point reference will be mapped into MGRS boxes to
allow for consistent aggregation, and adjustable density/fidelity. This
specification covers the use of 9-byte string form of 1km squares in the UTM
space (two digits of precision.)


### It is decidedly so

At the core, the M8B isa `map<id, set<coords>>`

In pseudocode, the construction of the map for an n-bit slice would look like:

```
  foreach ap 
    coords c = encode(ap.lat,ap.lon)
    digest h = hash(ap.mac)
    bits id = truncate(h,n) 
    set<coords> s = map.get(id) // collisions expected
    s.add(c) // collisions, dups expected
```

### Ask again later

Given a set of observed identifiers, deriving possible coordinates from the
artifact generated above would look like the following pseudocode:

```
  map<coords,int> sum

  foreach mac in macs
    digest h = hash(mac)
    bits id = truncate(h,n)
    set<coords> s = map.get(id)

    // histogram the results
    foreach c in s
      sum[c]++
```

sort sum by descending value, keys will be the decreasingly likely coordinate
for the set of macs.

### Very doubtful

Any encoding that captures (or allows reconstruction of) the map -> set
structure is sufficient. The simplest option will be a list of
(identifier, coordinate)
tuples. This form may be read in in entirety or if laid out in order,
searched in limited linear or binary fashion to isolate the observed values.

The file is structured in two parts, a header and the body.
The header is (8 utf8 encoded, newline terminated lines):

```
4 byte marker: "MJG\n" (4 byte magic number for m8b files)
version:       "2\n" (hex integer, this specification is "2")
hash:          "SIP-2-4\n" (string identifier, implies implementation/params)
slicebits:     "x\n" (hex bits)
coords:        "MGRS-1000\n" (1km square, implies scheme)
idsize:        "4\n" (hex bytes, extra leading bits 0)
coordsize:     "9\n" (hex bytes, extra leading bits 0)
record count:  "12a4\n" (hex count of records)
```

Followed immediately by the body of undelimited concatenated id coord pairs,
sorted by id ascending. This specification uses 4 byte x86 (Little Endian) integer
encoding for the Identifier and 9 bytes of MGRS string.

```
(<identifier><coordinate>)*
```

The above example header would be 36 bytes if slicebits was 16 (0x10)
followed directly by 4772 records (62036 bytes)

The fixed size encoding allows the file to be binary-searched inplace.

To decode: read the header for parameters, and read the records to reconstruct the map.

.m8b vs
.➑  (\u2791)

## How to build
### By hand
```
m8b$ cd src/main/java
java$ javac net/wigle/m8b/m8b.java
```
### With Maven
```
m8b$
mvn clean compile install
```

## How to use
### By hand
```
m8b/src/main/java$ java -cp . net.wigle.m8b.m8b   # as '$m8b' below
```

### From JAR
```
m8b/src$ java -jar target/m8b-1.0-SNAPSHOT.jar    # or your tag as '$m8b' below
```

### In Either Case
Magic (8) Balls are created using sets of netid-coordinate tuples as mentioned above. The default file format is pipe-delimited (tabs also supported via commandline option):

```
netid1|latitude1|longitude1
netid2|latitude2|longitude2
...
```

These files can be hashed into query-able m8b oracles, but you must also select your number of "slice-bits" - how lossy your compression will be / how large the artifact will be. WiGLE's reference implementations are released with a slice-bits of `0x20` - `32` bits.

```
# to CREATE an (8) ball oracle using disk based temp-space:
mkdir ./staging 				# staging temp dir; 'staging' used as an example
mkdir ./reduced					# reduction temp dir; 'reduced' used as an example
$m8b restage <observation-filename> ./staging/ && \
	$m8b reduce ./staging/ ./reduced/ <slice-bits> && \
	$m8b combine ./reduced/ <m8b-filename> <slice-bits>

# 'generate' is the faster, more memory-intensive equivalent of above.
# you must set -Xmx and -Xms java options based on your input size for this to work
$m8b generate <observation-filename> <m8b-filename> <slice-bits>

# to QUERY an (8) ball oracle
$m8b/src$ java -cp . net.wigle.m8b scan tb_v1.m8b.gz DE:86:2D:62:2A:45 DE:96:CD:80:35:0C DA:FD:A9:3A:EA:15
 (and so on)

# all commands and options:
$m8b stage observation-filename stage-location/ [-t]
$m8b restage observation-filename stage-location/ [-t]
$m8b score stage-location/
$m8b score2 stage-location/
$m8b reduce stage-location/ reduce-location/ slice-bits
$m8b compact stage-location/
$m8b combine reduce-location/ m8b-filename slice-bits
$m8b dumpi intermediate-filename
$m8b query m8b-filename mac1 [... macN]
$m8b scan m8b-filename mac1 [... macN]

# ('-t' option for tab-delimited source files, default is '|' delimited)

```
find the highest number of hits for an MGRS coordinate, that might be where those addresses are!

### WiGLE Wireless
The [WiGLE Wireless](https://github.com/wiglenet/wigle-wifi-wardriving) open source application for Android can export your observations as a shareable Magic (8) Ball if you want to use or transmit your observations to others. This is particularly useful should you intend to conduct an area survey upon which others (who trust you) can rely. Signing, securely transmitting, and sharing Magic (8) Balls is currently beyond the scope of the project, but is a direction for future exploration. You can obtain a verified copy of WiGLE Wireless through the Android [Play Store](https://play.google.com/store/apps/details?id=net.wigle.wigleandroid) or build your own from source. Apple's policy prohibits equivalent network detection packages from being released via the Apple Store at the time of publication.

## Potential Applications
M8Bs are infrastructure, not an end-product. We can't wait to see what you'll come up with. That being said, we see a few primary applications immediately:

 - Use m(8)b results to seed other location searches/lookups (GPS orbital prediction, low-resolution geofence checks, bounding for queries to off-board APIs)
 - Use m(8)b results to improve privacy: don't send probe requests (and leak information about yourself) for fixed wireless systems that aren't nearby
 - Define custom m(8)bs for specific applications or locations. This means you can verify and limit data as desired to deter malicious interference, minimize storage requirements, and develop networks of trusted data without WiGLE or any other provider as an intermediary.


## Artifacts
From time to time we may release partial extracts from the wigle dataset:
- https://github.com/wiglenet/m8binary/releases/tag/v1.0-aleph

