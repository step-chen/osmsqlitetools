package osmattr

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/paulmach/orb/encoding/wkt"
	"gopkg.in/yaml.v3"
)

type LinesSplitConfigs struct {
	Configs []LinesSplitConfig
}

type LinesSplitConfig struct {
	LineLayer string
	NodeLayer string
}

// This function reads the config file and returns the config struct
func loadConfigs(filename string) LinesSplitConfigs {
	data, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalln(err)
	}

	var conf LinesSplitConfigs
	// Unmarshal the data into the config struct
	err = yaml.Unmarshal(data, &conf)
	if err != nil {
		log.Fatalln(err)
	}

	return conf
}

func dropTmpTable(tblName string, db *sql.DB) {
	// Drop the temporary table if it exists
	strSql := fmt.Sprintf("DROP TABLE IF EXISTS %s", tblName)
	_, err := db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	// Run VACUUM to clean up the database and free up space
	_, err = db.Exec("VACUUM;")
	if err != nil {
		log.Fatalln(err)
	}
}

// CREATE TABLE 'lines' ( "ogc_fid" INTEGER PRIMARY KEY AUTOINCREMENT, 'osm_id' VARCHAR, 'name' VARCHAR, 'highway' VARCHAR, "GEOMETRY" LINESTRING)

// This function creates a temporary table that contains the split lines, with an index on ogc_fid.
func createTmpTable(c LinesSplitConfig, db *sql.DB) string {
	tblName := fmt.Sprintf("tmp_%s", c.LineLayer)
	idxName := fmt.Sprintf("idx_%s", tblName)

	dropTmpTable(tblName, db)

	strSql := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s", tblName, c.LineLayer)
	_, err := db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	strSql = fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (ogc_fid)", idxName, tblName)
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	return tblName
}

func SplitLines(strConfigFileName string, db *sql.DB) {
	conf := loadConfigs(strConfigFileName)
	for _, c := range conf.Configs {
		createNodes(c, db, false)

		tmpTblName := createTmpTable(c, db)

		strSql := fmt.Sprintf("SELECT lines_fid, line_id, order_id FROM %s WHERE intersections > 1 AND pos_type = 0 ORDER BY lines_fid ASC, line_id ASC, order_id ASC", c.NodeLayer)
		rowsNodes, err := db.Query(strSql)
		if err != nil {
			log.Fatalln(err)
		}
		defer rowsNodes.Close()

		tx, err := db.Begin()
		if err != nil {
			log.Fatalln(err)
		}

		lastRFID := int64(-1)

		var (
			rfID     int64
			lineIDs  []int
			orderIDs []int
		)

		strCols := getColsSql(tmpTblName, db)
		for rowsNodes.Next() {
			var (
				lineID  int
				orderID int
			)
			if err := rowsNodes.Scan(&rfID, &lineID, &orderID); err != nil {
				log.Fatal(err)
			}
			if lastRFID == -1 {
				lastRFID = rfID
			}

			if lastRFID != rfID {
				split(lastRFID, lineIDs, orderIDs, strCols, tmpTblName, c, db, tx)
				lastRFID = rfID
				lineIDs = []int{}
				orderIDs = []int{}
				lineIDs = append(lineIDs, lineID)
				orderIDs = append(orderIDs, orderID)
			} else {
				lineIDs = append(lineIDs, lineID)
				orderIDs = append(orderIDs, orderID)
			}
		}

		split(rfID, lineIDs, orderIDs, strCols, tmpTblName, c, db, tx)

		if err := rowsNodes.Err(); err != nil {
			log.Fatalln(err)
		}

		err = tx.Commit()
		if err != nil {
			log.Fatalln(err)
		}

		dropTmpTable(tmpTblName, db)
		createNodes(c, db, true)
	}
}

func split(ogcFid int64, lineIDs []int, pntIDs []int, strCols string, tblName string, c LinesSplitConfig, db *sql.DB, tx *sql.Tx) {
	strSql := fmt.Sprintf("SELECT AsBinary(GEOMETRY) FROM %s WHERE ogc_fid=?", tblName)
	row := db.QueryRow(strSql, ogcFid)
	if row.Err() != nil {
		log.Fatalln(row.Err())
	}

	var geomData []byte
	if err := row.Scan(&geomData); err != nil {
		log.Fatalln(err)
	}

	geom, err := wkb.Unmarshal(geomData)
	if err != nil {
		log.Fatalln(err)
	}

	mls, ok := geom.(orb.MultiLineString)
	if ok {
		splitMultiLine(ogcFid, mls, lineIDs, pntIDs, strCols, tblName, c, tx)
		return
	}
	line, ok := geom.(orb.LineString)
	if ok {
		splitLine(ogcFid, line, pntIDs, strCols, tblName, c, tx)
		return
	}
	log.Fatalln("Geometry is not a MultiLineString or LineString")
}

func splitLine(ogcFid int64, line orb.LineString, pntIDs []int, strCols string, tblName string, c LinesSplitConfig, tx *sql.Tx) {
	// osm 9930461
	ml := make([]orb.LineString, len(pntIDs)+1)
	count := len(pntIDs)

	group := [2]int{0, 0}
	for i := 0; i <= count; i++ {
		group[0] = group[1]
		if i == count {
			group[1] = len(line) - 1
		} else {
			group[1] = pntIDs[i]
		}

		for idP := group[0]; idP <= group[1]; idP++ {
			ml[i] = append(ml[i], line[idP])
		}
	}

	for i, l := range ml {
		err := error(nil)
		strMl := wkt.MarshalString(l)
		if i == 0 {
			strSql := fmt.Sprintf("UPDATE %s SET GEOMETRY = GeomFromText(?, 4326) WHERE ogc_fid = ?", c.LineLayer)
			_, err = tx.Exec(strSql, strMl, ogcFid)
		} else {
			strSql := fmt.Sprintf(`INSERT INTO %s (%s, GEOMETRY) SELECT %s, GeomFromText(?, 4326) AS GEOMETRY FROM %s WHERE ogc_fid = ?`, c.LineLayer, strCols, strCols, tblName)
			_, err = tx.Exec(strSql, strMl, ogcFid)
		}
		if err != nil {
			log.Fatalln(err)
		}
	}
}

func splitMultiLine(ogcFid int64, mls orb.MultiLineString, lineIDs []int, pntIDs []int, strCols string, tblName string, c LinesSplitConfig, tx *sql.Tx) {
	// osm 9930461
	ml := make([]orb.MultiLineString, len(lineIDs)+1)
	l := orb.LineString{}
	count := len(lineIDs)

	group := [2][2]int{{0, 0}, {0, 0}}
	for i := 0; i <= count; i++ {
		group[0] = group[1]
		if i == count {
			group[1] = [2]int{len(mls) - 1, len(mls[len(mls)-1]) - 1}
		} else {
			group[1] = [2]int{lineIDs[i], pntIDs[i]}
		}

		for idLS := group[0][0]; idLS <= group[1][0]; idLS++ {
			line := mls[idLS]
			if i == count && idLS > group[0][0] {
				ml[i] = append(ml[i], line)
			} else if idLS > group[0][0] && idLS < group[1][0] {
				ml[i] = append(ml[i], line)
			} else if idLS == group[0][0] {
				stopIdP := len(line)
				if idLS == group[1][0] {
					stopIdP = group[1][1]
				}
				for idLP := group[0][1]; idLP <= stopIdP; idLP++ {
					// for idLP, pnt := range line {
					pnt := line[idLP]
					if idLP == group[0][1] {
						l = orb.LineString{}
						l = append(l, pnt)
					} else if idLP > group[0][1] && idLP < stopIdP {
						l = append(l, pnt)
					} else {
						l = append(l, pnt)
						ml[i] = append(ml[i], l)
					}
				}
			}
		}
	}

	for i, l := range ml {
		err := error(nil)
		strMl := wkt.MarshalString(l)
		if i == 0 {
			strSql := fmt.Sprintf("UPDATE %s SET GEOMETRY = GeomFromText(?, 4326) WHERE ogc_fid = ?", c.LineLayer)
			_, err = tx.Exec(strSql, strMl, ogcFid)
		} else {
			strSql := fmt.Sprintf(`INSERT INTO %s (%s, GEOMETRY) SELECT %s, GeomFromText(?, 4326) AS GEOMETRY FROM %s WHERE ogc_fid = ?`, c.LineLayer, strCols, strCols, tblName)
			_, err = tx.Exec(strSql, strMl, ogcFid)
		}
		if err != nil {
			log.Fatalln(err)
		}
	}
}

/*
WITH RECURSIVE split(ogc_fid, osm_id, GEOMETRY, i, geom) AS (
    SELECT ogc_fid, osm_id, GEOMETRY, 0 AS i, NULL as geom
    FROM lines
    UNION ALL
    SELECT ogc_fid, osm_id, GEOMETRY, i+1, ST_PointN(GEOMETRY, i+1) AS geom
    FROM split
    WHERE i < ST_NumPoints(GEOMETRY)
)
SELECT ogc_fid AS lines_fid, osm_id, i AS order_id,
CASE
    WHEN i == 1 THEN 1
    WHEN i == ST_NumPoints(GEOMETRY) THEN 2
    ELSE 0
END pos_type,
geom AS GEOMETRY
FROM split
WHERE geom IS NOT NULL ORDER BY ogc_fid, i
*/
// 332017161 192930 {"type":"LineString","coordinates":[[46.7427034,24.5241916],[46.7480282,24.52049239999999],[46.74831189999999,24.5202774],[46.74895459999999,24.5198424]]}
// 53433 {"type":"LineString","coordinates":[[46.7482234,24.5207416],[46.7480282,24.52049239999999]]}
func createNodes(c LinesSplitConfig, db *sql.DB, createOnlyEndpoint bool) {
	strSql := fmt.Sprintf("SELECT DiscardGeometryColumn('%s', 'GEOMETRY')", c.NodeLayer)
	_, err := db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}
	strSql = fmt.Sprintf("DROP TABLE IF EXISTS %s", c.NodeLayer)
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	// Assuming the nodes table does not exist and needs to be created
	strSql = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (ogc_fid INTEGER PRIMARY KEY AUTOINCREMENT, lines_fid INTEGER, osm_id BIGINT, line_id INTEGER, order_id INTEGER, pos_type INTEGER DEFAULT 0, intersections INTEGER DEFAULT 0)", c.NodeLayer)
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}
	strSql = fmt.Sprintf("SELECT AddGeometryColumn('%s', '%s', 4326, 'POINT', 'XY', 1)", c.NodeLayer, "GEOMETRY")
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	// Query roads geometries
	strSql = fmt.Sprintf("SELECT ogc_fid, osm_id, AsBinary(GEOMETRY) FROM %s", c.LineLayer)
	rows, err := db.Query(strSql)
	if err != nil {
		log.Fatalln(err)
	}
	defer rows.Close()

	tx, err := db.Begin()
	if err != nil {
		log.Fatalln(err)
	}

	for rows.Next() {
		var rfID int64
		var osmID int64
		var geomData []byte
		if err := rows.Scan(&rfID, &osmID, &geomData); err != nil {
			log.Fatalln(err)
		}

		geom, err := wkb.Unmarshal(geomData)
		if err != nil {
			log.Fatalln(err)
		}

		mls, ok := geom.(orb.MultiLineString)
		if ok {
			for lineID, line := range mls {
				strSql = fmt.Sprintf("INSERT INTO %s (lines_fid, osm_id, line_id, order_id, pos_type, GEOMETRY) VALUES (?, ?, ?, ?, ?, GeomFromText(?))", c.NodeLayer)
				if createOnlyEndpoint {
					_, err := tx.Exec(strSql, rfID, osmID, lineID, 0, 1, fmt.Sprintf("POINT(%f %f)", line[0].Lon(), line[0].Lat()))
					if err != nil {
						log.Fatalln(err)
					}
					_, err = tx.Exec(strSql, rfID, osmID, lineID, 1, 2, fmt.Sprintf("POINT(%f %f)", line[len(line)-1].Lon(), line[len(line)-1].Lat()))
					if err != nil {
						log.Fatalln(err)
					}
				}
				for orderID, point := range line {
					// Insert each point into the nodes table
					pos_type := 0
					if orderID == 0 {
						pos_type = 1
					} else if orderID == len(line)-1 {
						pos_type = 2
					}
					_, err := tx.Exec(strSql, rfID, osmID, lineID, orderID, pos_type, fmt.Sprintf("POINT(%f %f)", point.Lon(), point.Lat()))

					if err != nil {
						log.Fatalln(err)
					}
				}
			}

			continue
		}

		line, ok := geom.(orb.LineString)
		if ok {
			strSql = fmt.Sprintf("INSERT INTO %s (lines_fid, osm_id, line_id, order_id, pos_type, GEOMETRY) VALUES (?, ?, ?, ?, ?, GeomFromText(?, 4326))", c.NodeLayer)
			if createOnlyEndpoint {
				strPnt := wkt.MarshalString(line[0])
				_, err := tx.Exec(strSql, rfID, osmID, 0, 0, 1, strPnt)
				if err != nil {
					log.Fatalln(err)
				}
				strPnt = wkt.MarshalString(line[len(line)-1])
				_, err = tx.Exec(strSql, rfID, osmID, 0, 1, 2, strPnt)
				if err != nil {
					log.Fatalln(err)
				}
			} else {
				for orderID, point := range line {
					// Insert each point into the nodes table
					pos_type := 0
					if orderID == 0 {
						pos_type = 1
					} else if orderID == len(line)-1 {
						pos_type = 2
					}
					strPnt := wkt.MarshalString(point)
					_, err := tx.Exec(strSql, rfID, osmID, 0, orderID, pos_type, strPnt)

					if err != nil {
						log.Fatalln(err)
					}
				}
			}

			continue
		}

		log.Fatalln("Geometry is not a MultiLineString or LineString")
	}

	if err := rows.Err(); err != nil {
		log.Fatalln(err)
	}

	err = tx.Commit()
	if err != nil {
		log.Fatalln(err)
	}

	strSql = fmt.Sprintf("CREATE INDEX idx_osm_id ON %s (osm_id ASC)", c.NodeLayer)
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	strSql = fmt.Sprintf("CREATE INDEX idx_geo ON %s (GEOMETRY ASC)", c.NodeLayer)
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	strSql = fmt.Sprintf("UPDATE %s SET intersections = (SELECT COUNT(*) FROM %s AS ln2 WHERE ln2.GEOMETRY = %s.GEOMETRY)", c.NodeLayer, c.NodeLayer, c.NodeLayer)
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}
}

func getColsSql(tblName string, db *sql.DB) (strCols string) {
	strSql := fmt.Sprintf("SELECT name FROM pragma_table_info('%s')", tblName)
	rows, err := db.Query(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	strCol := ""

	for rows.Next() {
		if err := rows.Scan(&strCol); err != nil {
			log.Fatalln(err)
		}

		if strCol == "ogc_fid" {
			continue
		} else if strCol == "GEOMETRY" {
			//strCol = "AsBinary(GEOMETRY)"
			continue
		}

		if len(strCols) == 0 {
			strCols += strCol
		} else {
			strCols += fmt.Sprintf(", %s", strCol)
		}
	}

	return strCols
}
