package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/mattn/go-sqlite3"
	OAT "navinfo.com/osmsqlitetools/internal/pkg/osmattr"
	OL2T "navinfo.com/osmsqlitetools/internal/pkg/osmnode"
)

// First to convert osm to spatialite
// ogr2ogr -f SQLite route.sqlite route.osm -progress -dsco SPATIALITE=YES
// go run main.go -f "./samples/route1.sqlite" -t "./tags.yml" -e "./lines_extract.yml" -s "./lines_split.yml"

var (
	showUsage          bool
	strPathName        string
	strTagConfPathName string
	strExtConfPathName string
	strSptConfPathName string
)

func usage() {
	fmt.Fprintf(os.Stderr, `OSM tools version: gosmt/1.0.0
Usage: gosmt [-hs] [-f "osm spatialite filename"] [-t "config file name"]

Options:
`)
	flag.PrintDefaults()
}

func init() {
	flag.BoolVar(&showUsage, "h", false, "Show help.")
	flag.StringVar(&strPathName, "f", "", "Set spatialite file name.")
	flag.StringVar(&strTagConfPathName, "t", "", "Set tag extract config file name.")
	flag.StringVar(&strExtConfPathName, "e", "", "Set lines extract config file name.")
	flag.StringVar(&strSptConfPathName, "s", "", "Split lines at intersection config file name.")

	flag.Usage = usage
}

func main() {
	log.SetFlags(log.Lshortfile)

	flag.Parse()

	if len(strings.TrimSpace(strPathName)) == 0 {
		log.Println("The file name of osm spatialite should not empty")
		showUsage = true
	}

	if showUsage {
		flag.Usage()
		return
	}

	sql.Register("sqlite3_with_spatialite",
		&sqlite3.SQLiteDriver{
			Extensions: []string{"mod_spatialite"},
		})
	strSql := fmt.Sprintf("file:%s?cache=shared&mode=rwc&_fk=1", strPathName)
	db, err := sql.Open("sqlite3_with_spatialite", strSql)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	if len(strExtConfPathName) > 0 {
		OAT.ExtractLines(strExtConfPathName, db)
	}

	if len(strTagConfPathName) > 0 {
		OAT.ExtractTags(strTagConfPathName, db)
		/*strTags := OAT.FetchAllTags("lines", db)
		log.Println(strTags)
		strTags = OAT.FetchAllTags("other_relations", db)
		log.Println(strTags)
		strTags = OAT.FetchAllTags("points", db)
		log.Println(strTags)
		*/
	}

	if len(strSptConfPathName) > 0 {
		OL2T.SplitLines(strSptConfPathName, db)
	}
}
