package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/PuerkitoBio/goquery"
	_ "github.com/microsoft/go-mssqldb"

	"qfc_inflation_tracker.com/inflation_tracking/internal/DatabaseCredentials"
	"qfc_inflation_tracker.com/inflation_tracking/internal/QfcTypes"
)

// Broadway Market QFC
var QfcLocationId string = "70500887"

func FindPriceForItem(item *QfcTypes.QfcItem) (uint64, error) {

	// Do a get request
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("https://www.qfc.com/p/%s", item.QfcUrl), nil)
	if err != nil {
		return 0, fmt.Errorf("ItemNew request failed. %w", err)
	}

	// Add necessary headers in.
	req.Header.Add("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36")
	req.Header.Add("accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9")
	req.Header.Add("accept-language", "en")
	req.Header.Add("cookie", fmt.Sprintf("x-active-modality={\"type\":\"PICKUP\",\"locationId\":\"%s\",\"source\":\"MODALITY_OPTIONS\",\"createdDate\":1661013991615}", QfcLocationId))

	// Make the actual request
	timeout, _ := time.ParseDuration("10s")
	client := http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("Doing the request failed. %w", err)
	}
	defer resp.Body.Close()

	// Create a new document to query
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("Creating new document failed. %w", err)
	}

	// Find price in document
	findPriceInDoc := func(query string) (uint64, error) {
		priceMatches := doc.Find(query).Map(func(idx int, s *goquery.Selection) string {
			return strings.Map(func(r rune) rune {
				if unicode.IsDigit(r) {
					return r
				} else {
					return -1
				}
			}, s.Text())
		})
		// span#ProductDetails-sellBy-weight
		if len(priceMatches) != 1 {
			return 0, fmt.Errorf("Price was not unique in doc. Query was \"%s\". Found %d matches.", query, len(priceMatches))
		}

		price, err := strconv.ParseUint(priceMatches[0], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("Converting price to cents failed. Err: %w.", err)
		}

		return price, nil
	}

	var priceInCents uint64
	if item.SoldByWeight {
		priceInCents, err = findPriceInDoc("span#ProductDetails-sellBy-weight")
		if err != nil {
			return 0, fmt.Errorf("Finding price in doc failed. %w", err)
		}
	} else {
		// Dollar amount part
		dollarPartOfPrice, err := findPriceInDoc("label[for='PICKUP'] .kds-Price-promotional-dropCaps")
		if err != nil {
			return 0, fmt.Errorf("Finding dollar part of price in doc failed. %w", err)
		}

		centPartOfPrice, err := findPriceInDoc("label[for='PICKUP'] .kds-Price-superscript:last-child")
		if err != nil {
			return 0, fmt.Errorf("Finding cent part of price in doc failed. %w", err)
		}

		priceInCents = centPartOfPrice + dollarPartOfPrice*100
	}

	return priceInCents, nil
}

func main() {
	file, err := os.OpenFile("scrapper_cron_job.log", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	log.SetOutput(file)
	log.SetPrefix(fmt.Sprintf("[pid %d] ", os.Getpid()))
	log.Printf("Job started.")

	// Connect to Azure SQL
	cred := DatabaseCredentials.GetDatabaseCredentialsFromEnv()
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;database=%s;",
		cred.ServerAddr,
		cred.User,
		cred.Password,
		cred.Port,
		cred.Database)

	var db *sql.DB
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
	}
	defer db.Close()

	ctx := context.Background()

	for i := 0; i < 5; i++ {
		err = db.PingContext(ctx)
		if err != nil {
			log.Printf("Pinging context failed. Retrying...: %s \n", err.Error())
			time.Sleep(10 * time.Second)
		} else {
			break
		}
	}
	if err != nil {
		log.Fatal("Pinging context failed: ", err.Error())
	}

	fmt.Printf("Connected!\n")

	// Set up products to track
	queryStatement := `
		SELECT local_id, description, qfc_url, sold_by_weight, added_timestamp FROM qfc_items
	`
	query, err := db.PrepareContext(ctx, queryStatement)
	if err != nil {
		log.Fatal("Error preparing context: ", err.Error())
	}
	defer query.Close()

	rows, err := query.QueryContext(ctx)
	if err != nil {
		log.Fatal("Error querying context: ", err.Error())
	}
	defer rows.Close()

	items := []QfcTypes.QfcItem{}
	for rows.Next() {
		var item QfcTypes.QfcItem
		err := rows.Scan(&item.LocalId, &item.Description, &item.QfcUrl, &item.SoldByWeight, &item.AddedTimestamp)
		if err != nil {
			log.Fatal("Error scanning: ", err)
		}
		items = append(items, item)
	}

	// Prepare insert query
	insertStatement := `
		INSERT INTO qfc_prices (item_local_id, price_in_cents, qfc_location_id, timestamp) VALUES (@LocalId, @PriceInCents, @LocationId, SYSUTCDATETIME());
	`
	insertQuery, err := db.PrepareContext(ctx, insertStatement)
	if err != nil {
		log.Fatalf("db.PrepareQuery for insert. %s\n", err.Error())
	}
	defer insertQuery.Close()

	// For each product and uri
	successes := 0
	failures := 0
	for _, item := range items {
		// Find price in the web page.
		priceInCents, err := FindPriceForItem(&item)
		if err != nil {
			log.Printf("[error item %s] %s \n", item.Description, err.Error())
			failures += 1
			continue
		}

		// Put entry in postgresql (insert).
		row := insertQuery.QueryRowContext(
			ctx,
			sql.Named("LocalId", item.LocalId),
			sql.Named("PriceInCents", priceInCents),
			sql.Named("LocationId", QfcLocationId),
		)

		err = row.Err()
		if err != nil {
			log.Printf("[error item %s] %s \n", item.Description, err.Error())
			failures += 1
			continue
		}

		fmt.Printf("%s: $%d.%d \n", item.Description, priceInCents/100, priceInCents%100)
		successes += 1
	}

	log.Printf("Job finished. %d prices successfully queried, %d failed.\n", successes, failures)
	fmt.Printf("Job finished. %d prices successfully queried, %d failed.\n", successes, failures)
}
