package main

// TODO: https://medium.com/insiderengineering/a-pragmatic-and-systematic-project-structure-in-go-4a47b4fbe929

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "modernc.org/sqlite"
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

func getRows(query string) (*sql.Rows, error) {
	db, err := sql.Open("sqlite", "meteo.db") // not dealing with "warning mode" for the time being
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

func getClosestCity(lat float32, lon float32) {

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

}

func getCoordinateForecasts(w http.ResponseWriter, r *http.Request) {
	log.Println(r.URL.RequestURI())

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
	mux := http.NewServeMux()                                            // https://pkg.go.dev/net/http#ServeMux
	mux.HandleFunc("/privacy-policy", getPrivacyPolicy)                  // http://localhost:3333/privacy-policy?lang=en
	mux.HandleFunc("/api/v1/forecast/cities", getCoordinateForecasts)    // http://localhost:3333/api/v1/forecast/cities?lat=56.9730&lon=24.1327
	mux.HandleFunc("/api/v1/forecast/cities/name", getCityNameForecasts) // http://localhost:3333/api/v1/forecast/cities/name?city_name=vamier

	err := http.ListenAndServe("localhost:3333", mux)
	if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}
}
