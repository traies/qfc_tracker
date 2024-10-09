package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/microsoft/go-mssqldb"
	"qfc_inflation_tracker.com/inflation_tracking/internal/DatabaseCredentials"
	"qfc_inflation_tracker.com/inflation_tracking/internal/QfcTypes"
)

func main() {
	// Set up products to track
	// id: int, description: varchar(MAX), qfc_url: varchar(MAX), sold_by_weight: boolean
	items := []QfcTypes.QfcItem{
		{LocalId: 1, Description: "Bananas per pound", QfcUrl: "banana/0000000004011", SoldByWeight: true},
		{LocalId: 2, Description: "Coke zero 2l", QfcUrl: "coca-cola-zero-sugar-soda/0004900005014", SoldByWeight: false},
		{LocalId: 3, Description: "Darigold fat free milk 64 fl oz", QfcUrl: "darigold-zero-ultra-pasteurized-fat-free-milk/0002640029601", SoldByWeight: false},
		{LocalId: 4, Description: "Nescafe clasico 7 oz", QfcUrl: "nescafe-clasico-dark-roast-instant-coffee/0002800046631", SoldByWeight: false},
		{LocalId: 5, Description: "Best foods mayo squeeze 20 oz", QfcUrl: "best-foods-real-mayo-squeeze-bottle-real-mayonnaise/0004800135449", SoldByWeight: false},
		{LocalId: 6, Description: "Tabasco Original Pepper 5 fl oz", QfcUrl: "tabasco-original-pepper-hot-sauce/0001121000015", SoldByWeight: false},
		{LocalId: 7, Description: "Frank's RedHot Original 12 fl oz", QfcUrl: "frank-s-redhot-original-cayenne-pepper-sauce/0004150080502", SoldByWeight: false},
		{LocalId: 8, Description: "Heinz Tomato Ketchup 32 oz", QfcUrl: "heinz-tomato-ketchup/0001300000605", SoldByWeight: false},
		{LocalId: 9, Description: "French's classic yellow mustard 14 oz", QfcUrl: "french-s-classic-yellow-mustard/0004150000025", SoldByWeight: false},
		{LocalId: 10, Description: "Land O Lakes salted butter sticks 1 lb", QfcUrl: "land-o-lakes-salted-butter-sticks/0003450015164", SoldByWeight: false},
		{LocalId: 11, Description: "QFC Large white eggs 18 ct", QfcUrl: "qfc-large-white-eggs/0001111060922", SoldByWeight: false},
		{LocalId: 12, Description: "Crisco vegetable oil 40 fl oz", QfcUrl: "crisco-vegetable-oil/0019600570833", SoldByWeight: false},
		{LocalId: 13, Description: "LaCroix lemon sparkling water 8 cans/12 fl oz", QfcUrl: "lacroix-lemon-sparkling-water/0001299322130", SoldByWeight: false},
		{LocalId: 14, Description: "Applegate Natural Uncured Genoa Salami Sliced 4 oz", QfcUrl: "applegate-natural-uncured-genoa-salami-sliced/0002531785600", SoldByWeight: false},
		{LocalId: 15, Description: "Lay's classic potato chips 8 oz", QfcUrl: "lay-s-classic-potato-chips/0002840019914", SoldByWeight: false},
		{LocalId: 16, Description: "Degree men ultraclear fresh antiprespirant 3.8 oz", QfcUrl: "degree-men-ultraclear-fresh-antiperspirant-dry-spray/0007940067027", SoldByWeight: false},
		{LocalId: 17, Description: "Cheez it original cheese 12.4 oz", QfcUrl: "cheez-it-original-cheese-crackers/0002410010685", SoldByWeight: false},
		{LocalId: 18, Description: "Cheez it grooves sharp white 9 oz", QfcUrl: "cheez-it-grooves-sharp-white-cheddar-cheese-crackers/0002410059475", SoldByWeight: false},
		{LocalId: 19, Description: "Haagen Dazs mint chocolate chip 14 fl oz", QfcUrl: "haagen-dazs-mint-chocolate-chip-gluten-free-ice-cream/0007457008005", SoldByWeight: false},
		{LocalId: 20, Description: "Kroger crinkle cut french fries 32 oz", QfcUrl: "kroger-crinkle-cut-french-fries-bag/0001111087508", SoldByWeight: false},
		{LocalId: 21, Description: "Honey Nut Cheerios cereal 10.8 oz", QfcUrl: "big-g-cereal-gluten-free-honey-nut-cheerios-cereal/0001600012479", SoldByWeight: false},
		{LocalId: 22, Description: "La Brea Bakery French Loaf 14.5 oz", QfcUrl: "la-brea-bakery-french-loaf/0078142102430", SoldByWeight: false},
		{LocalId: 23, Description: "Dave's Killer Bread Thin-sliced Organic White Bread 20.5 oz", QfcUrl: "dave-s-killer-bread-white-bread-done-right-thin-sliced-organic-white-bread/0001376402808", SoldByWeight: false},
		{LocalId: 24, Description: "Kroger Pure Cane Granulated Sugar 4 lb", QfcUrl: "kroger-pure-cane-granulated-sugar/0001111088318", SoldByWeight: false},
		{LocalId: 25, Description: "Kroger Liquid Stevia Sweetener 3.38 oz", QfcUrl: "kroger-liquid-stevia-sweetener/0001111000702", SoldByWeight: false},
		{LocalId: 26, Description: "Doritos Nacho Cheese Flavored Tortilla Chips 9.25 oz", QfcUrl: "doritos-nacho-cheese-flavored-tortilla-chips/0002840051646", SoldByWeight: false},
		{LocalId: 27, Description: "La Banderita Family Pack Flour Tortillas 20 ct", QfcUrl: "la-banderita-family-pack-flour-tortillas/0002733100032", SoldByWeight: false},
		{LocalId: 28, Description: "La Banderita White Corn Tortillas 18 ct", QfcUrl: "la-banderita-white-corn-tortillas/0002733100060", SoldByWeight: false},
		{LocalId: 29, Description: "Oreo Chocolate Sandwich Cookies Family Size 19.1 oz", QfcUrl: "oreo-chocolate-sandwich-cookies-family-size/0004400003327", SoldByWeight: false},
		{LocalId: 30, Description: "Sara Lee Honey Wheat Sandwich Bread 20 oz", QfcUrl: "sara-lee-honey-wheat-sandwich-bread/0007294560136", SoldByWeight: false},
		{LocalId: 31, Description: "Barilla Rigatoni Pasta 1 lb", QfcUrl: "barilla-rigatoni-pasta/0007680850294", SoldByWeight: false},
		{LocalId: 32, Description: "Classico Tomato & Basil Pasta Sauce 24 oz", QfcUrl: "classico-tomato-basil-pasta-sauce/0004112907712", SoldByWeight: false},
		{LocalId: 33, Description: "Kroger Marinara Pasta Sauce 24 oz", QfcUrl: "classico-tomato-basil-pasta-sauce/0004112907712", SoldByWeight: false},
		{LocalId: 34, Description: "Bertolli d'Italia Marinara Sauce 24.7 oz", QfcUrl: "bertolli-d-italia-marinara-sauce/0003620043153", SoldByWeight: false},
		{LocalId: 35, Description: "Scott Comfort Plus Toilet Paper Double Rolls 1 Ply Toilet Tissue 12 ct", QfcUrl: "scott-comfort-plus-toilet-paper-double-rolls-1-ply-toilet-tissue/0005400047618", SoldByWeight: false},
		{LocalId: 36, Description: "Bounty Double Roll Select-A-Size White Paper Towels 6 rolls", QfcUrl: "bounty-double-roll-select-a-size-white-paper-towels/0003700066557", SoldByWeight: false},
		{LocalId: 37, Description: "Colgate Cavity Protection Regular Flavor Anticavity Fluoride Toothpaste 4 oz", QfcUrl: "colgate-cavity-protection-regular-flavor-anticavity-fluoride-toothpaste/0003500051406", SoldByWeight: false},
		{LocalId: 38, Description: "Snickers Chocolate Candy Bars Fun Size 10.59 oz", QfcUrl: "snickers-chocolate-candy-bars-fun-size/0004000050533", SoldByWeight: false},
		{LocalId: 39, Description: "KitKat Milk Chocolate Snack Size Wafer Candy Bars Halloween Bag 1 bag / 10.78 oz", QfcUrl: "kit-kat-milk-chocolate-snack-size-wafer-candy-bars-halloween-bag/0003400008752", SoldByWeight: false},
		{LocalId: 40, Description: "Reese's Milk Chocolate Peanut Butter Snack Size Cups Halloween Bag 1 bag / 10.5 oz", QfcUrl: "reese-s-milk-chocolate-peanut-butter-snack-size-cups-candy-halloween-bag/0003400040211", SoldByWeight: false},
		{LocalId: 41, Description: "Kroger Sweet Whole Kernel Golden Corn 15.25 oz", QfcUrl: "kroger-sweet-whole-kernel-golden-corn/0001111080803", SoldByWeight: false},
		{LocalId: 42, Description: "Kroger Garden Variety Sweet Peas 15 oz", QfcUrl: "kroger-garden-variety-sweet-peas/0001111081211", SoldByWeight: false},
		{LocalId: 43, Description: "Applegate Natural Oven Roasted Turkey Breast 7 oz", QfcUrl: "applegate-natural-oven-roasted-turkey-breast/0002531758600", SoldByWeight: false},
		{LocalId: 44, Description: "Hillshire Farm Ultra Thin Sliced Honey Ham Lunch Meat 9 oz", QfcUrl: "hillshire-farm-ultra-thin-sliced-honey-ham-lunch-meat/0004450097648", SoldByWeight: false},
		{LocalId: 45, Description: "Applegate Natural Black Forest Uncured Ham 7 oz", QfcUrl: "applegate-natural-black-forest-uncured-ham/0002531700590", SoldByWeight: false},
		{LocalId: 46, Description: "Draper Valley All Natural Fresh Chicken Breast per pound", QfcUrl: "draper-valley-all-natural-fresh-chicken-breast/0025066550000", SoldByWeight: true},
		{LocalId: 47, Description: "Simple Truth Natural Air Chilled Boneless Skinless Chicken Breast per pound", QfcUrl: "simple-truth-natural-air-chilled-boneless-skinless-chicken-breast/0020058150000", SoldByWeight: true},
		{LocalId: 48, Description: "Simple Truth Organic Air Chilled Boneless Skinless Chicken Breast per pound", QfcUrl: "simple-truth-organic-air-chilled-boneless-skinless-chicken-breasts/0029082900000", SoldByWeight: true},
		{LocalId: 49, Description: "Beef Angus Choice Top Round Steak Value Pack (About 3 Steaks Per Pack) per pound", QfcUrl: "beef-angus-choice-top-round-steak-value-pack-about-3-steaks-per-pack-/0027237050000", SoldByWeight: true},
		{LocalId: 50, Description: "Certified Angus Beef Choice Top Sirloin Steak (1 Steak) per pound", QfcUrl: "certified-angus-beef-choice-top-sirloin-steak-1-steak-/0027223750000", SoldByWeight: true},
		{LocalId: 51, Description: "Certified Angus Beef Choice Boneless Strip Steak (1 Steak) per pound", QfcUrl: "certified-angus-beef-choice-boneless-strip-steak-1-steak-/0027223650000", SoldByWeight: true},
		{LocalId: 52, Description: "Oscar Mayer Original Center Cut Bacon 12 oz", QfcUrl: "oscar-mayer-original-center-cut-bacon/0004470002268", SoldByWeight: false},
		{LocalId: 53, Description: "Honeycrip Apple per pound", QfcUrl: "honeycrisp-apple/0000000003283", SoldByWeight: true},
		{LocalId: 54, Description: "Large Gala Apple per pound", QfcUrl: "large-gala-apple/0000000004134", SoldByWeight: true},
		{LocalId: 55, Description: "Lemons per unit", QfcUrl: "lemons/0000000004053", SoldByWeight: false},
		{LocalId: 56, Description: "Organic Kale 16 oz", QfcUrl: "organic-kale/0000000094627", SoldByWeight: false},
		{LocalId: 57, Description: "Russet Potato per pound", QfcUrl: "russet-potato/0000000004072", SoldByWeight: true},
		{LocalId: 58, Description: "Red Potatoes per pound", QfcUrl: "red-potatoes/0000000004073", SoldByWeight: true},
		{LocalId: 59, Description: "Sweet onions per pound", QfcUrl: "sweet-onions/0000000004166", SoldByWeight: true},
		{LocalId: 60, Description: "Large green bell pepper per unit", QfcUrl: "large-green-bell-pepper/0000000004065", SoldByWeight: false},
		{LocalId: 61, Description: "Roma Tomato per pound", QfcUrl: "roma-tomato/0000000004087", SoldByWeight: true},
		{LocalId: 62, Description: "Large Avocado per unit", QfcUrl: "large-avocado/0000000004225", SoldByWeight: false},
		{LocalId: 63, Description: "Medium Avocado per unit", QfcUrl: "medium-avocado/0000000004046", SoldByWeight: false},
		{LocalId: 64, Description: "Red Seedless Grapes per pound", QfcUrl: "red-seedless-grapes/0000000004023", SoldByWeight: true},
		{LocalId: 65, Description: "Cotton Candy Grapes per pound", QfcUrl: "cotton-candy-grapes/0000000003093", SoldByWeight: true},
		{LocalId: 66, Description: "Gum Drop Grapes per pound", QfcUrl: "gum-drop-grapes/0085046300425", SoldByWeight: true},
		{LocalId: 67, Description: "Pam Original No-Stick Cooking Spray 8 oz", QfcUrl: "pam-original-no-stick-cooking-spray/0006414403031", SoldByWeight: false},
		{LocalId: 68, Description: "Colavita Balsamic Vinager 34 fl oz", QfcUrl: "colavita-balsamic-vinegar/0003915341303", SoldByWeight: false},
		{LocalId: 69, Description: "Kikkoman soy sauce 10 fl oz", QfcUrl: "kikkoman-soy-sauce/0004139000002", SoldByWeight: false},
		{LocalId: 70, Description: "Sweet Baby Ray's Buffalo wing sauce 16 fl oz", QfcUrl: "sweet-baby-ray-s-buffalo-wing-sauce/0001340912844", SoldByWeight: false},
	}
	// Connect to Azure SQL
	cred := DatabaseCredentials.GetDatabaseCredentialsFromEnv()
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;database=%s;",
		cred.ServerAddr,
		cred.User,
		cred.Password,
		cred.Port,
		cred.Database)

	var db *sql.DB
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
	}
	defer db.Close()

	ctx := context.Background()
	err = db.PingContext(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}
	fmt.Printf("Connected!\n")

	// Try to insert every product one by one. If they already exist, ignore it.
	checkExistanceStatement := `
		SELECT COUNT(local_id) as ids_found FROM qfc_items WHERE local_id = @LocalId
	`
	checkExistanceQuery, err := db.Prepare(checkExistanceStatement)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer checkExistanceQuery.Close()

	insertStatement := `
		INSERT INTO qfc_items (local_id, description, qfc_url, sold_by_weight, added_timestamp) VALUES (@LocalId, @Description, @QfcUrl, @SoldByWeight, SYSUTCDATETIME());
	`

	insertQuery, err := db.Prepare(insertStatement)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer insertQuery.Close()

	inserted := 0
	for _, item := range items {
		row := checkExistanceQuery.QueryRowContext(ctx, sql.Named("LocalId", item.LocalId))
		var existingIds int
		err = row.Scan(&existingIds)
		if err != nil {
			log.Printf("Error checking existance of %s.", item.Description)
			continue
		}
		if existingIds > 0 {
			log.Printf("Skipping %s...", item.Description)
			continue
		}

		row = insertQuery.QueryRowContext(
			ctx,
			sql.Named("LocalId", item.LocalId),
			sql.Named("Description", item.Description),
			sql.Named("QfcUrl", item.QfcUrl),
			sql.Named("SoldByWeight", item.SoldByWeight))

		err = row.Err()
		if err != nil {
			log.Printf("Error inserting %s: %s\n", item.Description, err.Error())
		} else {
			log.Printf("Inserted %s.", item.Description)
			inserted += 1
		}
	}

	log.Printf("Inserted %d new items.", inserted)
}
