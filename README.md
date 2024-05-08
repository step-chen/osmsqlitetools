# osmsqlitetools
This contains functions for working with OpenStreetMap (OSM) data using spatialite/sqlite.
## osmnode
Split the lines in the OSM data with the intersection nodes.
## osmattr
Extract the attribute with the lines from tag in the lines.
## Example
### First to convert osm to spatialite using ogr2ogr
```bash
ogr2ogr -f SQLite route.sqlite route.osm -progress -dsco SPATIALITE=YES
```
### Run with tools with the giving yaml configure file
```bash
go run main.go -f "./samples/route1.sqlite" -t "./tags.yml" -e "./lines_extract.yml" -s "./lines_split.yml"
```
