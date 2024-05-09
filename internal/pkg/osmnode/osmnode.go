package osmnode

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
	LineLayer     string
	LineNodeLayer string
	NodeLayer     string
}

func loadConfigs(filename string) LinesSplitConfigs {
	data, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalln(err)
	}

	var conf LinesSplitConfigs
	err = yaml.Unmarshal(data, &conf)
	if err != nil {
		log.Fatalln(err)
	}

	return conf
}

func dropTmpTable(tblName string, db *sql.DB) {
	strSql := fmt.Sprintf("DROP TABLE IF EXISTS %s", tblName)
	_, err := db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	_, err = db.Exec("VACUUM;")
	if err != nil {
		log.Fatalln(err)
	}
}

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
		createLineNode(c, db, false)

		tmpTblName := createTmpTable(c, db)

		strSql := fmt.Sprintf("SELECT lines_fid, order_id FROM %s WHERE intersections > 1 AND pos_type = 0 ORDER BY lines_fid ASC, order_id ASC", c.LineNodeLayer)
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

		log.Println("Start split line with intersection nodes")

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

		log.Println("Finished split line with intersection nodes")

		dropTmpTable(tmpTblName, db)
		createLineNode(c, db, true)
		createNode(c, db)
		createNodeRef(c, db)
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

func createLineNode(c LinesSplitConfig, db *sql.DB, createOnlyEndpoint bool) {
	log.Println("Start create line' node")

	strSql := fmt.Sprintf("SELECT DropGeoTable('%s')", c.LineNodeLayer)
	_, err := db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}
	strSql = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (ogc_fid INTEGER PRIMARY KEY AUTOINCREMENT, lines_fid INTEGER, osm_id BIGINT, order_id INTEGER, pos_type INTEGER DEFAULT 0, node_fid INTEGER, intersections INTEGER)", c.LineNodeLayer)
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}
	strSql = fmt.Sprintf("SELECT AddGeometryColumn('%s', '%s', 4326, 'POINT', 'XY', 1)", c.LineNodeLayer, "GEOMETRY")
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	if createOnlyEndpoint {
		strSql = fmt.Sprintf(`
			WITH RECURSIVE nodes (ogc_fid, osm_id, num, order_id, geom) AS (
				SELECT ogc_fid, osm_id, ST_NumPoints(GEOMETRY), 1, ST_PointN(GEOMETRY, 1) AS geom FROM %s
			UNION ALL
				SELECT ogc_fid, osm_id, ST_NumPoints(GEOMETRY), ST_NumPoints(GEOMETRY), ST_PointN(GEOMETRY, ST_NumPoints(GEOMETRY)) AS geom FROM lines
			)
			INSERT INTO %s (lines_fid, osm_id, order_id, pos_type, GEOMETRY)
				SELECT ogc_fid AS lines_fid, osm_id, order_id,
					CASE WHEN order_id == 1 THEN 1 WHEN order_id == num THEN 2 ELSE 0 END pos_type,
					geom AS GEOMETRY FROM nodes WHERE geom IS NOT NULL ORDER BY ogc_fid, order_id`,
			c.LineLayer, c.LineNodeLayer)
	} else {
		strSql = fmt.Sprintf(`
			WITH RECURSIVE nodes(ogc_fid, osm_id, GEOMETRY, i, geom) AS (
				SELECT ogc_fid, osm_id, GEOMETRY, 0 AS i, NULL as geom FROM %s
				UNION ALL
				SELECT ogc_fid, osm_id, GEOMETRY, i+1, ST_PointN(GEOMETRY, i+1) AS geom	FROM nodes
				WHERE i < ST_NumPoints(GEOMETRY)
			)
			INSERT INTO %s (lines_fid, osm_id, order_id, pos_type, GEOMETRY)
				SELECT ogc_fid AS lines_fid, osm_id, i AS order_id,
					CASE WHEN i == 1 THEN 1 WHEN i == ST_NumPoints(GEOMETRY) THEN 2 ELSE 0 END pos_type,
					geom AS GEOMETRY FROM nodes WHERE geom IS NOT NULL ORDER BY ogc_fid, i`,
			c.LineLayer, c.LineNodeLayer)
	}
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	strSql = fmt.Sprintf("CREATE INDEX idx_osm_id ON %s (osm_id ASC)", c.LineNodeLayer)
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}
	strSql = fmt.Sprintf("CREATE INDEX idx_ln_geo ON %s (GEOMETRY ASC)", c.LineNodeLayer)
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}
	strSql = fmt.Sprintf("SELECT CreateSpatialIndex('%s', '%s')", c.LineNodeLayer, "GEOMETRY")
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}
	strSql = fmt.Sprintf("UPDATE %s SET intersections = (SELECT COUNT(*) FROM %s AS ln2 WHERE ln2.GEOMETRY = %s.GEOMETRY)", c.LineNodeLayer, c.LineNodeLayer, c.LineNodeLayer)
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Finished create line' node")
}

func createNode(c LinesSplitConfig, db *sql.DB) {
	log.Println("Start create node")

	strSql := fmt.Sprintf("SELECT DropGeoTable('%s')", c.NodeLayer)
	_, err := db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}
	strSql = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (ogc_fid INTEGER PRIMARY KEY AUTOINCREMENT, intersections INTEGER)", c.NodeLayer)
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}
	strSql = fmt.Sprintf("SELECT AddGeometryColumn('%s', '%s', 4326, 'POINT', 'XY', 1)", c.NodeLayer, "GEOMETRY")
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}
	strSql = fmt.Sprintf(`INSERT INTO %s (intersections, GEOMETRY) SELECT intersections, GEOMETRY FROM %s GROUP BY GEOMETRY`, c.NodeLayer, c.LineNodeLayer)
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}
	strSql = fmt.Sprintf("CREATE INDEX idx_nodes_geo ON %s (GEOMETRY ASC)", c.NodeLayer)
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}
	strSql = fmt.Sprintf("SELECT CreateSpatialIndex('%s', '%s')", c.NodeLayer, "GEOMETRY")
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Finished create node")
}

func createNodeRef(c LinesSplitConfig, db *sql.DB) {
	log.Println("Start create ref between line and node")

	strSql := fmt.Sprintf("UPDATE %s SET node_fid = (SELECT ogc_fid FROM %s WHERE %s.GEOMETRY=%s.GEOMETRY)", c.LineNodeLayer, c.NodeLayer, c.LineNodeLayer, c.NodeLayer)
	_, err := db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	/*strSql = fmt.Sprintf("UPDATE %s SET intersections = (SELECT COUNT(*) FROM %s WHERE %s.ogc_fid = %s.node_fid)", c.NodeLayer, c.LineNodeLayer, c.NodeLayer, c.LineNodeLayer)
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}*/

	strSql = fmt.Sprintf("SELECT DiscardGeometryColumn('%s', '%s')", c.LineNodeLayer, "GEOMETRY")
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	strSql = fmt.Sprintf("DROP INDEX %s", "idx_ln_geo")
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	strSql = fmt.Sprintf("ALTER TABLE %s DROP COLUMN GEOMETRY", c.LineNodeLayer)
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	strSql = fmt.Sprintf("ALTER TABLE %s DROP COLUMN intersections", c.LineNodeLayer)
	_, err = db.Exec(strSql)
	if err != nil {
		log.Fatalln(err)
	}

	_, err = db.Exec("VACUUM;")
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Finished create ref between line and node")
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
