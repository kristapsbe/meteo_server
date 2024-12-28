package main

// TODO: https://medium.com/insiderengineering/a-pragmatic-and-systematic-project-structure-in-go-4a47b4fbe929

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v3"
	// TODO https://turriate.com/articles/making-sqlite-faster-in-go
	sqlite3 "github.com/mattn/go-sqlite3" // https://github.com/cvilsmeier/go-sqlite-bench - should probs switch
)

const HourlyParams = `
	'Laika apstākļu piktogramma',
	'Temperatūra (°C)',
	'Sajūtu temperatūra (°C)',
	'Vēja ātrums (m/s)',
	'Vēja virziens (°)',
	'Brāzmas (m/s)',
	'Nokrišņi (mm)',
	'UV indekss (0-10)',
	'Pērkona negaisa varbūtība (%)'
`

const DailyParams = `
	'Diennakts vidējā vēja vērtība (m/s)',
	'Diennakts maksimālā vēja brāzma (m/s)',
	'Diennakts maksimālā temperatūra (°C)',
	'Diennakts minimālā temperatūra (°C)',
	'Diennakts nokrišņu summa (mm)',
	'Diennakts nokrišņu varbūtība (%)',
	'Laika apstākļu piktogramma nakti',
	'Laika apstākļu piktogramma diena'
`

type City struct {
	id       string
	name     string
	lat      float32
	lon      float32
	ctype    string
	distance float32
}

type CityForecast struct {
}

func getRows(db *sql.DB, query string) (*sql.Rows, error) {
	rows, err := db.Query(query)

	if err != nil {
		return nil, err
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return rows, nil
}

func getParams(db *sql.DB, paramQ string) (*sql.Rows, error) {
	return getRows(db, fmt.Sprintf(`
  		SELECT
            id, title_lv, title_en
        FROM
            forecast_cities_params
        WHERE
            title_lv in ('%s')
    `, paramQ))
}

func isEmergency() bool {
	_, err := os.Stat("run_emergency")
	return err == nil
}

func getLocationRange(forceAll bool) string {
	if forceAll || !isEmergency() {
		return "('republikas pilseta', 'citas pilsētas', 'rajona centrs', 'pagasta centrs', 'ciems')"
	} else {
		return "('republikas pilseta', 'citas pilsētas', 'rajona centrs')"
	}
}

func getClosestCity(db *sql.DB, lat float64, lon float64, distance int, forceAll bool, ignoreDistance bool) (City, error) {
	whereString := ""
	if !ignoreDistance && lat > 55.7 && lat < 58.05 && lon > 20.95 && lon < 28.25 {
		whereString = fmt.Sprintf(`
			WHERE
            	distance <= (%d/ctype)
	    `, distance)
	}

	rows, err := getRows(db, fmt.Sprintf(`
		WITH city_distances AS (
            SELECT
                id,
                name,
                lat,
                lon,
                CASE type
                    WHEN 'republikas pilseta' THEN 1
                    WHEN 'citas pilsētas' THEN 2
                    WHEN 'rajona centrs' THEN 3
                    WHEN 'pagasta centrs' THEN 4
                    WHEN 'ciems' THEN 5
                END as ctype,
                ACOS((SIN(RADIANS(lat))*SIN(RADIANS(%f)))+(COS(RADIANS(lat))*COS(RADIANS(%f)))*(COS(RADIANS(%f)-RADIANS(lon))))*6371 as distance
            FROM
                cities
            WHERE
                type in %s
        )
        SELECT
            id, name, lat, lon, ctype, distance
        FROM
            city_distances
        %s
        ORDER BY
            ctype ASC, distance ASC
        LIMIT 1
    `, lat, lat, lon, getLocationRange(forceAll), whereString))

	if err != nil {
		return City{}, err
	}

	city := City{}
	if rows.Next() {
		if err := rows.Scan(&city.id, &city.name, &city.lat, &city.lon, &city.ctype, &city.distance); err == nil {
			log.Print("city")
			return city, nil
		} else { // dealing with cases where you've got no cities near you
			if ignoreDistance {
				log.Print("ignore dist ")
				return city, err
			} else {
				log.Print("go deeper")
				return getClosestCity(db, lat, lon, distance, forceAll, true)
			}
		}
	} else {
		if ignoreDistance {
			log.Print("ignore dist no res")
			return city, sql.ErrNoRows
		} else {
			log.Print("go deeper no res")
			return getClosestCity(db, lat, lon, distance, forceAll, true)
		}
	}
}

func getClosestCityByName(name string) {

}

func getForecast() {

}

func getWarnings() {

}

func getSimpleWarnings() {

}

func getAuroraProbability() {

}

func getCityResponse() {
	//hourlyParams, err := getParams(HourlyParams)
	//dailyParams, err := getParams(DailyParams)
}

func getCityForecasts(c fiber.Ctx, db *sql.DB) string {
	log.Println(c.OriginalURL())

	lat, err := strconv.ParseFloat(strings.TrimSpace(c.Query("lat")), 64)
	if err != nil {
		return err.Error()
	}

	lon, err := strconv.ParseFloat(strings.TrimSpace(c.Query("lon")), 64)
	if err != nil {
		return err.Error()
	}

	city, err := getClosestCity(db, lat, lon, 10, true, false)
	if err != nil {
		return err.Error()
	}
	return city.name
}

func getCityNameForecasts(w http.ResponseWriter, r *http.Request) {
	log.Println(r.URL.RequestURI())

}

func getPrivacyPolicy(w http.ResponseWriter, r *http.Request) {
	log.Println(r.URL.RequestURI())

	p := "./privacy_policy/privacy-policy.html"
	if r.URL.Query().Get("lang") == "lv" {
		p = "./privacy_policy/privatuma-politika.html"
	}
	http.ServeFile(w, r, p)
}

func main() {
	app := fiber.New()

	sql.Register("sqlite3_extended",
		&sqlite3.SQLiteDriver{
			Extensions: []string{
				"/Users/kristaps/.sqlpkg/sqlite/spellfix/spellfix.dylib",
				"/Users/kristaps/.sqlpkg/nalgeon/math/math.dylib",
			},
		},
	)

	max_conns := 5 // TODO: conn pool may be pointless
	conns := make(chan *sql.DB, max_conns)

	for i := 0; i < max_conns; i++ {
		conn, _ := sql.Open("sqlite3_extended", "file:meteo.db?cache=shared&mode=ro")

		defer func() {
			conn.Close()
		}()
		conns <- conn
	}

	checkout := func() *sql.DB {
		return <-conns
	}

	checkin := func(c *sql.DB) {
		conns <- c
	}

	// http://localhost:3333/privacy-policy?lang=en
	app.Get("/privacy-policy", func(c fiber.Ctx) error {
		log.Println(c.OriginalURL())

		p := "./privacy_policy/privacy-policy.html"
		if c.Query("lang") == "lv" {
			p = "./privacy_policy/privatuma-politika.html"
		}
		return c.SendFile(p)
	})

	// http://localhost:3333/api/v1/forecast/cities?lat=56.9730&lon=24.1327
	app.Get("/api/v1/forecast/cities", func(c fiber.Ctx) error {
		db := checkout()
		defer checkin(db)

		return c.SendString(getCityForecasts(c, db))
	})

	// http://localhost:3333/api/v1/forecast/cities/name?city_name=vamier
	app.Get("/api/v1/forecast/cities/name", func(c fiber.Ctx) error {
		return c.SendString("Hello, World 👋!")
	})

	log.Fatal(app.Listen(":3333"))
}
