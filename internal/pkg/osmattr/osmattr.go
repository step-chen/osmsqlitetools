package osmattr

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type TagsConfigs struct {
	Configs []TagsConfig
}

type TagsConfig struct {
	Layer string
	Ref   string
	Tags  []Tag
}

type Tag struct {
	Name  string
	Field string
	Type  string
}

type LinesExtractConfigs struct {
	Configs []LinesExtractConfig
}

type LinesExtractConfig struct {
	Layer     string
	Table     string
	Field     string
	SubField  string
	ExtFields []LinesExtractField
}

type LinesExtractField struct {
	Field string
	Value string
}

func loadLinesExtractConfigs(filename string) LinesExtractConfigs {
	conf := LinesExtractConfigs{}

	data, err := os.ReadFile(filename)
	if err != nil {
		log.Fatal(err)
	}

	err = yaml.Unmarshal(data, &conf)
	if err != nil {
		log.Fatal(err)
	}

	return conf
}

func loadTagConfigs(filename string) TagsConfigs {
	conf := TagsConfigs{}

	data, err := os.ReadFile(filename)
	if err != nil {
		log.Fatal(err)
	}

	err = yaml.Unmarshal(data, &conf)
	if err != nil {
		log.Fatal(err)
	}

	return conf
}

func ExtractTags(strConfigFileName string, db *sql.DB) {
	conf := loadTagConfigs(strConfigFileName)

	/*for _, c := range conf.Configs {
		for _, t := range c.Tags {
			strAlt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", c.Layer, t.Field, t.Type)
			_, err := db.Exec(strAlt)
			if err != nil {
				if strings.Contains(err.Error(), "duplicate column name") {
					log.Println(err.Error())
				} else {
					log.Fatalln(err.Error())
				}
			}
		}
	}*/

	for _, c := range conf.Configs {
		createTagTable(c, db)
		rows, err := db.Query("SELECT osm_id, other_tags FROM " + c.Layer + " WHERE other_tags IS NOT NULL")
		if err != nil {
			log.Fatalln(err.Error())
		}
		defer rows.Close()

		tx, err := db.Begin()
		if err != nil {
			log.Fatalln(err.Error())
		}

		for rows.Next() {
			var (
				osmid   int64
				strTags string
			)
			if err := rows.Scan(&osmid, &strTags); err != nil {
				log.Fatalln(err)
			}

			m := make(map[string]string)
			kvPairs := strings.Split(strTags, ",")
			for _, pair := range kvPairs {
				parts := strings.Split(pair, "=>")
				if len(parts) == 2 {
					k := strings.Trim(parts[0], `"`)
					v := strings.Trim(parts[1], `"`)
					m[k] = v
				}
			}
			strCol := ""
			strVal := ""
			for _, t := range c.Tags {
				v, ok := m[t.Name]
				if ok {
					strCol += `, ` + t.Field
					strVal += `, "` + v + `"`
				}
			}

			if len(strCol) > 0 {
				strCol = "osm_id" + strCol
				strVal = fmt.Sprintf("%d", osmid) + strVal
				strSql := fmt.Sprintf("INSERT INTO %s (%s) VALUES ( %s )", c.Ref, strCol, strVal)
				_, err = tx.Exec(strSql)
				if err != nil {
					log.Fatalln(err.Error())
				}
			}
		}

		err = tx.Commit()
		if err != nil {
			log.Fatalln(err.Error())
		}
		/*strSql := fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_%s ON %s (ogc_fid)", c.Ref, c.Ref)
		_, err = db.Exec(strSql)
		if err != nil {
			log.Fatal(err)
		}*/
	}
}

/*func dropTmpTable(c Config, db *sql.DB) {
	tblName := `t_` + c.Layer
	_, err := db.Exec("DROP TABLE IF EXISTS " + tblName)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("VACUUM;")
	if err != nil {
		log.Fatal(err)
	}
}*/

func createTagTable(c TagsConfig, db *sql.DB) {
	_, err := db.Exec(`DROP TABLE IF EXISTS ` + c.Ref)
	if err != nil {
		log.Fatal(err)
	}

	//strCreate := fmt.Sprintf("CREATE TABLE %s ( ogc_fid INTEGER PRIMARY KEY AUTOINCREMENT, osm_id INTEGER REFERENCES lines (osm_id) ON DELETE CASCADE ON UPDATE CASCADE", c.Ref)
	strCreate := fmt.Sprintf("CREATE TABLE %s ( ogc_fid INTEGER PRIMARY KEY AUTOINCREMENT, osm_id INTEGER", c.Ref)
	for _, t := range c.Tags {
		strCreate += ", " + t.Field + " " + t.Type
	}
	strCreate += " )"

	_, err = db.Exec(strCreate)
	if err != nil {
		log.Fatal(err)
	}
}

/*
waterway   VARCHAR,
aerialway  VARCHAR,
barrier    VARCHAR,
man_made   VARCHAR,
railway    VARCHAR,
*/
func ExtractLines(strConfigFileName string, db *sql.DB) {
	conf := loadLinesExtractConfigs(strConfigFileName)
	for _, c := range conf.Configs {
		for _, f := range c.ExtFields {
			strSql := ""
			strWhere := fmt.Sprintf("%s IS NOT NULL", f.Field)
			if len(f.Value) > 0 {
				if f.Value == "NULL" {
					strWhere = fmt.Sprintf("%s IS NULL", f.Field)
				} else {
					strWhere = fmt.Sprintf("%s='%s'", f.Field, f.Value)
				}
			}
			if f.Field != "highway" {
				strWhere += " AND highway IS NULL"
			}
			if isTblExist(c.Table, db) {
				strSql = fmt.Sprintf("INSERT INTO %s(ogc_fid, osm_id, name, %s, %s, z_order, other_tags, GEOMETRY) SELECT ogc_fid, osm_id, name, '%s' AS %s, %s AS %s, z_order, other_tags, GEOMETRY FROM %s WHERE %s", c.Table, c.Field, c.SubField, f.Field, c.Field, f.Field, c.SubField, c.Layer, strWhere)
			} else {
				strSql = fmt.Sprintf("CREATE TABLE %s AS SELECT ogc_fid, osm_id, name, '%s' AS %s, %s AS %s, z_order, other_tags, GEOMETRY FROM %s WHERE %s", c.Table, f.Field, c.Field, f.Field, c.SubField, c.Layer, strWhere)
			}

			_, err := db.Exec(strSql)
			if err != nil {
				log.Fatalln(err.Error())
			}

			strSql = fmt.Sprintf("DELETE FROM %s WHERE %s", c.Layer, strWhere)
			_, err = db.Exec(strSql)
			if err != nil {
				log.Fatalln(err.Error())
			}

			if len(f.Value) == 0 {
				strSql = fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", c.Layer, f.Field)
				_, err = db.Exec(strSql)
				if err != nil {
					log.Fatalln(err.Error())
				}
			}
		}
	}
}

/*
lines: ["tower:construction" "oneway" "layer" "generator:source" "old_ref" "crossing" "club" "gauge" "tunnel" "was:landuse" "generator:type" "pipeline" "note:source" "width" "cycleway:right" "oneway:bicycle" "alt_name:en" "old_name" "gas_insulated" "name:grc" "cables" "footway" "psv" "abandoned" "noname" "name:ckb" "embankment" "to" "wires" "subway" "landuse" "tower:type" "was:sport" "handrail" "railway:etcs" "cutting" "was:golf" "usage" "operator" "alt_name_gmap" "historic" "foot" "sidewalk" "substation" "name:fa" "left:country" "bicycle" "fence_type" "bridge" "name:source" "railway:track_ref" "name:ar" "circuits" "generator:output:electricity" "lanes" "boat" "frequency" "generator:method" "wall" "amenity" "disused:highway" "vehicle" "cycleway" "postal_code" "abandoned:highway" "bus" "incline" "roof:shape" "tracks" "destination" "skateway" "intermittent" "maxweight:signed" "train" "voltage" "twoway" "description" "building:material" "was:barrier" "electrified" "nan" "proposed" "was:leisure" "healthcare" "surface" "name:ar1" "step_count" "emergency" "horse" "colour" "leaf_type" "place" "direction" "lit" "par" "aeroway" "destination:ref" "facility" "name:en" "lanes:forward" "addr:street" "motorroad" "turn:lanes" "crossing:markings" "passenger_lines" "website" "name:fr" "name:en1" "height" "stack" "bicycle_road" "construction" "indoor" "name:tr" "turn" "sport" "name:de" "addr:city" "ford" "religion" "cycleway:left" "maxheight" "was:highway" "crossing:island" "boundary" "show_region" "operator:wikidata" "ref" "phone" "tracktype" "plant:output:electricity" "was:building" "line" "wdb:source" "maritime" "traffic_calming" "tactile_paving" "maxspeed" "name:sr" "area" "area:highway" "parking" "access" "plant:source" "public_transport" "leisure" "alt_name:ar" "attraction" "plant:method" "playground" "level" "station" "living_street" "destination:en" "lane_markings" "golf" "conveying" "backrest" "trail_visibility" "natural" "disused:area:aeroway" "service" "wikidata" "substance" "handicap" "was:waterway" "name:ur" "addr:housenumber" "int_name" "motor_vehicle" "label" "name:pa" "opening_hours" "plant:type" "bridge:structure" "roof:material" "show_label" "proposed:leisure" "railway:traffic_mode" "start_date" "right:country" "roof:colour" "material" "border_type" "junction" "smoothness" "location" "covered" "power" "passing_places" "name:tk" "admin_level" "alt_name" "wikipedia" "building:part"]
other_relations: ["restriction" "site" "waterway" "historic" "public_transport" "name:en"]
points: ["name:ur" "diet:halal" "cash_in" "name:diq" "sport" "capacity" "fitness_station" "backrest" "crossing:light" "operator:wikidata" "Fixme:de" "fuel:octane_92" "toilets:wheelchair" "name:la" "name:ms" "GNS:dsg_name" "line_management" "parking" "voltage" "name:rn" "railway" "playground" "diet:local" "alt_name" "addr:state" "name:ca" "name:simple" "access" "airmark" "check_date" "was:sport" "diet:kosher" "name:he" "name:sa" "name:te" "transformer" "cuisine:outside" "name:or" "name:zh-Hant" "name:zu" "generator:method" "generator:output:electricity" "bar" "name:lv" "cuisine:inhouse" "website:menu" "door" "payment:lightning" "name:lbe" "name:sco" "name:szl" "contact:email" "addr:country" "layer" "craft" "departures_board" "direction" "name:lmo" "name:sl" "admin_level" "capital" "construction" "lamp_type" "ford" "surface" "name:io" "name:kl" "name:lzh" "name:tk" "name:zh-Hans" "name:an" "name:cy" "name:kn" "payment:american_express" "jpoi_id" "service:vehicle:car_repair" "fixme:type" "addr:housename" "name:crh" "name:cs" "name:roa-tara" "official_name:ar" "brand" "organic" "building:material" "name:hu" "name:tr" "official_name:be" "currency:others" "stroller" "aerialway" "name:gag" "name:sr" "name:ug" "name:xal" "exit" "beds" "name:ka" "name:kk" "payment:maestro" "kids_area" "payment:apple_pay" "service:vehicle:transmission" "name:ko" "crossing:markings" "working" "name:bat-smg" "company" "service:vehicle:used_car_sales" "name:fr" "name:ks" "name:ml" "official_name:el" "kids_area:fee" "name:de" "name:roa-rup" "population:date" "healthcare" "name:et" "name:pt" "name:ro" "name:rue" "psv" "name:av" "name:bug" "name:mr" "population" "internet_access" "residential" "service:vehicle:car_parts" "official_name:pl" "name:mzn" "Transport" "beacon:type" "service" "denotation" "name:fa" "name:ta" "stars" "flag:name" "flag:type" "building:levels:underground" "name:az" "name:bxr" "name:ee" "name:ext" "kids_area:outdoor" "name:si" "brand:wikidata" "fuel:octane_95" "manufacturer" "covered" "name:ba" "name:ff" "payment:visa" "addr:place" "payment:visa_debit" "contact:phone" "local_ref" "name:gd" "name:pap" "name:sah" "natural" "underground" "name:scn" "name:war" "name:mhr" "name:ha" "name:nds" "not:brand:wikidata" "payment:cards" "addr:street:ar" "monitoring:ozone" "name:bar" "name:qu" "name:sg" "wikidata" "second_hand" "contact:linkedin" "name:da" "name:my" "name:nan" "indoor" "communication:mobile_phone" "name:ang" "name:na" "religion" "payment:applypay" "frequency" "contact:instagram" "payment:onchain" "name:ce" "name:chr" "brewery" "kerb" "website" "name:smn" "name:wuu" "service:vehicle:air_conditioning" "name:ki" "flag:wikidata" "location" "junction" "name:ak" "name:eu" "name:gl" "name:ht" "name:ku" "name:lb" "name:pa" "name:vo" "short_name" "clothes" "female" "seats" "addr:housenumber" "network" "name:sms" "name:to" "GNS:id" "artwork_type" "payment:cash" "image:thumb" "name:lg" "name:lo" "drive_through" "level" "name:bo" "brand:wikipedia" "dispensing" "grades" "attraction" "service:vehicle:body_repair" "official_name:it" "bicycle" "shelter_type" "name:frp" "cuisine" "train" "kids_area:indoor" "generator:type" "shelter" "official_name:br" "height" "building:use" "name:ar" "name:vec" "fee" "was:man_made" "service:vehicle:painting" "name:dv" "name:kv" "name:pnb" "name:zh" "official_name" "official_name:id" "official_name:et" "int_name" "payment:mastercard" "name:ar:-1970" "lamp_mount" "name:fo" "name:nah" "name:-1970" "landuse" "name:sq" "abandoned:aeroway" "contact:website" "addr:province" "material" "picture" "name:ps" "official_name:en" "addr:city:en" "name:ia" "fuel:octane_98" "embassy" "name:mk" "name:ie" "maxspeed" "animal_boarding" "name:af" "addr:district" "rooms" "image" "payment:google_pay" "name:gan" "name:it" "supervised" "alt_name_1" "name:ky" "takeaway" "drink:coffee" "place:-1970" "tower:type" "information" "roof:shape" "brand:ja" "name2" "name:ace" "name:nn" "name:vi" "name:zh_pinyin" "leisure" "type" "side" "currency:XBT" "taxon:family" "name:is" "name:ksh" "name:sw" "official_name:lt" "crossing:bell" "subject" "service:vehicle:brakes" "name:oc" "name:sh" "traffic_signals:direction" "payment:coins" "diet:meat" "service:vehicle:Car_sales" "instagram" "name:ceb" "name:rw" "name:sn" "name:tt" "name:uk" "name:vro" "foot" "bench" "service:vehicle:electrical" "tourism" "museum" "internet_access:fee" "operator:wikipedia" "name:cv" "name:id" "name:zea" "payment:mada" "diet:healthy" "country_code_fips" "outdoor_seating" "mofa" "name:gn" "name:ln" "subject:wikidata" "swimming_pool" "crossing" "power" "generator:source" "locked" "name:bg" "official_name:pt" "alt_name:ar" "wikipedia:de" "url" "trees" "addr:district:en" "name:ilo" "name:pam" "name:ru" "smoking" "design" "station" "start_date" "motor_vehicle" "sqkm" "kids_area:supervised" "name:bs" "name:hy" "subway" "operator" "waterway" "building:levels" "animal_breeding" "check_date:currency:XBT" "name:arc" "name:dsb" "alt_name:en" "club" "moped" "air_conditioning" "holding_position:type" "name:pms" "name:ti" "payment:debit_cards" "building:colour" "name:fy" "name:ss" "official_name:lb" "beauty" "name:so" "drinking_water" "name:gu" "motorcycle" "service:vehicle:oil_change" "addr:floor" "public_transport" "contact:twitter" "office" "name:als" "atm" "delivery" "light_rail" "diet:vegetarian" "telecom" "guest_house" "name:br" "name:jbo" "name:yue" "gate" "name:pdc" "name:tok" "source:population" "designation" "traffic_calming" "contact:facebook" "self_service" "name:lt" "name:tzl" "official_name:fr" "name:hsb" "name:yo" "artist_name" "communication:5G" "content" "addr:postcode" "addr:street" "alt_name:eo" "name:es" "name:hi" "traffic_signals" "phone" "wifi" "phases" "military" "name:jv" "name:kbd" "name:mn" "name:tg" "aeroway" "indoor_seating" "name:bcl" "official_name:af" "amenity" "police" "service:vehicle:repairs" "name:lez" "entrance" "vending" "currency:SAR" "payment:contactless" "name:be-tarask" "name:bm" "name:li" "wikipedia" "opening_hours" "resort" "name:tl" "bus" "lit" "addr:district:ar" "crossing:island" "name:yi" "wheelchair" "diet:chicken" "name:csb" "historic" "horse" "name:be" "name:fur" "description" "old_name" "name:ckb" "name:el" "service:vehicle:truck_repair" "changing_table" "name:wo" "communication:mobile" "addr:city" "alt_name:vi" "name:fi" "name:lfn" "mobile" "name:haw" "boundary" "diet:vegan" "name:am" "healthcare:speciality" "payment:mastercard_contactless" "fast_food" "name:bpy" "name:no" "diplomatic" "communication:gsm" "payment:electronic_purses" "service:vehicle:glass_repair" "name:hak" "name:nrm" "name:rm" "name:th" "name:udm" "GNS:dsg_code" "motorcar" "name:dz" "payment:telephone_cards" "service:vehicle:tyres_repair" "rating" "network:wikidata" "name:ga" "name:kab" "name:nv" "name:uz" "shop" "repair" "diet:organic" "contact:snapchat" "name:lij" "denomination" "source:name" "tower:construction" "service:vehicle:tyres" "name:ja" "email" "diet:gluten_free" "name:gv" "name:mt" "name:os" "fuel:octane_91" "elevator" "school" "amenity_1" "operator:type" "leaf_type" "name:en" "payment:visa_electron" "country" "name:eo" "name:ne" "name:nov" "brand:en" "name:nl" "was:leisure" "male" "name:km" "branch" "name:hif" "name:kw" "noexit" "old_ref" "target" "maxstay" "opening_hours:covid19" "name:sv" "emergency" "name:hr" "payment:credit_cards" "fax" "reservation" "name:arz" "name:ast" "name:pl" "brand:ar" "اتصالات" "consulting" "ISO3166-1:alpha2" "name:su" "building" "government" "trade" "fuel:diesel" "name:bn" "name:mg" "name:se" "name:sk" "disused:railway"]
*/
func FetchAllTags(tbl string, db *sql.DB) []string {
	if !isColExist(tbl, "other_tags", db) {
		return []string{}
	}

	rows, err := db.Query("SELECT other_tags FROM " + tbl + " WHERE other_tags IS NOT NULL")
	if err != nil {
		log.Println(err)
		return []string{}
	}
	defer rows.Close()

	m := make(map[string]interface{})
	// Iterate over the rows and print all the other_tags values
	for rows.Next() {
		var otherTags string
		if err := rows.Scan(&otherTags); err != nil {
			log.Println(err)
			continue
		}
		keyValuePairs := strings.Split(otherTags, ",")
		for _, pair := range keyValuePairs {
			parts := strings.Split(pair, "=>")
			if len(parts) == 2 {
				key := parts[0]
				value := parts[1]
				m[key] = value
			}
		}
	}

	tags := make([]string, 0, len(m))
	for k := range m {
		tags = append(tags, k)
	}
	return tags
}

func isTblExist(tbl string, db *sql.DB) bool {
	// Check if the table exists
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", tbl).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}
	if count == 0 {
		return false
	}

	return true
}

func isColExist(tbl string, col string, db *sql.DB) bool {
	// Check if the table exists
	if !isTblExist(tbl, db) {
		return false
	}

	// Check if the column exists in the table
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM pragma_table_info(?) WHERE name=?", tbl, col).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}
	if count == 0 {
		return false
	}

	return true
}
