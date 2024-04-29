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

		strSql := fmt.Sprintf("SELECT lines_fid, order_id FROM %s WHERE intersections > 1 AND pos_type = 0 ORDER BY lines_fid ASC, order_id ASC", c.NodeLayer)
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
			orderIDs []int
		)

		strCols := getColsSql(tmpTblName, db)
		for rowsNodes.Next() {
			var (
				orderID int
			)
			if err := rowsNodes.Scan(&rfID, &orderID); err != nil {
				log.Fatal(err)
			}
			if lastRFID == -1 {
				lastRFID = rfID
			}

			if lastRFID != rfID {
				split(lastRFID, orderIDs, strCols, tmpTblName, c, db, tx)
				lastRFID = rfID
				orderIDs = []int{}
				orderIDs = append(orderIDs, orderID)
			} else {
				orderIDs = append(orderIDs, orderID)
			}
		}

		split(rfID, orderIDs, strCols, tmpTblName, c, db, tx)

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

func split(ogcFid int64, pntIDs []int, strCols string, tblName string, c LinesSplitConfig, db *sql.DB, tx *sql.Tx) {
	strSql := fmt.Sprintf("SELECT ST_AsBinary(ST_DissolvePoints(GEOMETRY)) FROM %s WHERE ogc_fid=?", tblName)
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

	mp, ok := geom.(orb.MultiPoint)
	if !ok {
		log.Fatalln("Geometry is not a MultiLineString or LineString")
		return
	}

	var splitPnts orb.MultiPoint
	for _, id := range pntIDs {
		splitPnts = append(splitPnts, mp[id-1])
	}

	splitLine(ogcFid, splitPnts, strCols, tblName, c, db, tx)
}

// 10711
func splitLine(ogcFid int64, points orb.MultiPoint, strCols string, tblName string, c LinesSplitConfig, db *sql.DB, tx *sql.Tx) {
	strMp := wkt.MarshalString(points)
	strSql := fmt.Sprintf("SELECT ST_AsBinary(ST_LinesCutAtNodes(GEOMETRY, GeomFromText(?, 4326))) FROM %s WHERE ogc_fid == ?", tblName)
	row := db.QueryRow(strSql, strMp, ogcFid)
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

	ml, ok := geom.(orb.MultiLineString)
	if !ok {
		//log.Fatalln("Geometry is not a MultiLineString or LineString")
		return
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

// 332017161 192930 {"type":"LineString","coordinates":[[46.7427034,24.5241916],[46.7480282,24.52049239999999],[46.74831189999999,24.5202774],[46.74895459999999,24.5198424]]}
// 53433 {"type":"LineString","coordinates":[[46.7482234,24.5207416],[46.7480282,24.52049239999999]]}
func createNodes(c LinesSplitConfig, db *sql.DB, createOnlyEndpoint bool) {
	strSql := fmt.Sprintf("SELECT DropGeoTable('%s')", c.NodeLayer)
	_, err := db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	strSql = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (ogc_fid INTEGER PRIMARY KEY AUTOINCREMENT, lines_fid INTEGER, osm_id BIGINT, order_id INTEGER, pos_type INTEGER DEFAULT 0, intersections INTEGER DEFAULT 0)", c.NodeLayer)
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}
	strSql = fmt.Sprintf("SELECT AddGeometryColumn('%s', '%s', 4326, 'POINT', 'XY', 1)", c.NodeLayer, "GEOMETRY")
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	strSql = fmt.Sprintf(`WITH RECURSIVE split(ogc_fid, osm_id, GEOMETRY, i, geom) AS (
		SELECT ogc_fid, osm_id, GEOMETRY, 0 AS i, NULL as geom
		FROM %s
		UNION ALL
		SELECT ogc_fid, osm_id, GEOMETRY, i+1, ST_PointN(GEOMETRY, i+1) AS geom
		FROM split
		WHERE i < ST_NumPoints(GEOMETRY)
	)
	INSERT INTO %s (lines_fid, osm_id, order_id, pos_type, GEOMETRY)
	SELECT ogc_fid AS lines_fid, osm_id, i AS order_id,
	CASE
		WHEN i == 1 THEN 1
		WHEN i == ST_NumPoints(GEOMETRY) THEN 2
		ELSE 0
	END pos_type,
	geom AS GEOMETRY
	FROM split
	WHERE geom IS NOT NULL ORDER BY ogc_fid, i`, c.LineLayer, c.NodeLayer)
	_, err = db.Exec(strSql)
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
