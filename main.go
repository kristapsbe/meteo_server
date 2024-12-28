package main

// TODO: https://medium.com/insiderengineering/a-pragmatic-and-systematic-project-structure-in-go-4a47b4fbe929

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

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

func getRows(query string) (*sql.Rows, error) {
	db, err := sql.Open("sqlite3_extended", "meteo.db") // not dealing with "warning mode" for the time being
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(query)

	if err != nil {
		return nil, err
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	if err = db.Close(); err != nil {
		return nil, err
	}

	return rows, nil
}

func getParams(paramQ string) (*sql.Rows, error) {
	return getRows(fmt.Sprintf(`
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

func getClosestCity(lat float64, lon float64, distance int, forceAll bool, ignoreDistance bool) (City, error) {
	whereString := ""
	if !ignoreDistance && lat > 55.7 && lat < 58.05 && lon > 20.95 && lon < 28.25 {
		whereString = fmt.Sprintf(`
			WHERE
            	distance <= (%d/ctype)
	    `, distance)
	}

	rows, err := getRows(fmt.Sprintf(`
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
		if err := rows.Scan(&city); err == nil {
			log.Print("city")
			return city, nil
		} else { // dealing with cases where you've got no cities near you
			if ignoreDistance {
				log.Print("ignore dist ")
				return city, sql.ErrNoRows
			} else {
				log.Print("go deeper")
				return getClosestCity(lat, lon, distance, forceAll, true)
			}
		}
	} else {
		if ignoreDistance {
			log.Print("ignore dist no res")
			return city, sql.ErrNoRows
		} else {
			log.Print("go deeper no res")
			return getClosestCity(lat, lon, distance, forceAll, true)
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

func getCityForecasts(w http.ResponseWriter, r *http.Request) {
	log.Println(r.URL.RequestURI())

	lat, err := strconv.ParseFloat(strings.TrimSpace(r.URL.Query().Get("lat")), 64)
	if err != nil {
		io.WriteString(w, err.Error())
		return
	}
	log.Println(r.URL.RequestURI())

	lon, err := strconv.ParseFloat(strings.TrimSpace(r.URL.Query().Get("lon")), 64)
	if err != nil {
		io.WriteString(w, err.Error())
		return
	}
	log.Println(r.URL.RequestURI())

	city, err := getClosestCity(lat, lon, 10, true, false)
	io.WriteString(w, fmt.Sprint(city.name, err.Error()))
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
	sql.Register("sqlite3_extended",
		&sqlite3.SQLiteDriver{
			Extensions: []string{
				"/Users/kristaps/.sqlpkg/sqlite/spellfix/spellfix.dylib",
				"/Users/kristaps/.sqlpkg/nalgeon/math/math.dylib",
			},
		},
	)

	mux := http.NewServeMux()                                            // https://pkg.go.dev/net/http#ServeMux
	mux.HandleFunc("/privacy-policy", getPrivacyPolicy)                  // http://localhost:3333/privacy-policy?lang=en
	mux.HandleFunc("/api/v1/forecast/cities", getCityForecasts)          // http://localhost:3333/api/v1/forecast/cities?lat=56.9730&lon=24.1327
	mux.HandleFunc("/api/v1/forecast/cities/name", getCityNameForecasts) // http://localhost:3333/api/v1/forecast/cities/name?city_name=vamier

	err := http.ListenAndServe("localhost:3333", mux)
	if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}
}
