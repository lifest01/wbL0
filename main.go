package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"os"
	"time"
)

type OrderInfo struct {
	OrderUid          string    `json:"order_uid"`
	TrackNumber       string    `json:"track_number"`
	Entry             string    `json:"entry"`
	Delivery          Delivery  `json:"delivery"`
	Payment           Payment   `json:"payment"`
	Items             []Items   `json:"items"`
	Locale            string    `json:"locale"`
	InternalSignature string    `json:"internal_signature"`
	CustomerId        string    `json:"customer_id"`
	DeliveryService   string    `json:"delivery_service"`
	Shardkey          string    `json:"shardkey"`
	SmId              int       `json:"sm_id"`
	DateCreated       time.Time `json:"date_created"`
	OofShard          string    `json:"oof_shard"`
}

type Payment struct {
	Transaction  string `json:"transaction"`
	RequestId    string `json:"request_id"`
	Currency     string `json:"currency"`
	Provider     string `json:"provider"`
	Amount       int    `json:"amount"`
	PaymentDt    int    `json:"payment_dt"`
	Bank         string `json:"bank"`
	DeliveryCost int    `json:"delivery_cost"`
	GoodsTotal   int    `json:"goods_total"`
	CustomFee    int    `json:"custom_fee"`
}

type Delivery struct {
	Name    string `json:"name"`
	Phone   string `json:"phone"`
	Zip     string `json:"zip"`
	City    string `json:"city"`
	Address string `json:"address"`
	Region  string `json:"region"`
	Email   string `json:"email"`
}

type Items struct {
	ChrtId      int    `json:"chrt_id"`
	TrackNumber string `json:"track_number"`
	Price       int    `json:"price"`
	Rid         string `json:"rid"`
	Name        string `json:"name"`
	Sale        int    `json:"sale"`
	Size        string `json:"size"`
	TotalPrice  int    `json:"total_price"`
	NmId        int    `json:"nm_id"`
	Brand       string `json:"brand"`
	Status      int    `json:"status"`
}

func main() {
	urlExample := "postgres://lifest:12345@localhost:5432/wbl0"
	conn, err := pgx.Connect(context.Background(), urlExample)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	json_value := `{
		"order_uid": "b563feb7b2b84b6test",
			"track_number": "WBILMTESTTRACK",
			"entry": "WBIL",
			"delivery": {
			"name": "Test Testov",
				"phone": "+9720000000",
				"zip": "2639809",
				"city": "Kiryat Mozkin",
				"address": "Ploshad Mira 15",
				"region": "Kraiot",
				"email": "test@gmail.com"
		},
		"payment": {
			"transaction": "b563feb7b2b84b6test",
				"request_id": "",
				"currency": "USD",
				"provider": "wbpay",
				"amount": 1817,
				"payment_dt": 1637907727,
				"bank": "alpha",
				"delivery_cost": 1500,
				"goods_total": 317,
				"custom_fee": 0
		},
		"items": [
	{
	"chrt_id": 9934930,
	"track_number": "WBILMTESTTRACK",
	"price": 453,
	"rid": "ab4219087a764ae0btest",
	"name": "Mascaras",
	"sale": 30,
	"size": "0",
	"total_price": 317,
	"nm_id": 2389212,
	"brand": "Vivienne Sabo",
	"status": 202
	},
{
	"chrt_id": 441293,
	"track_number": "WBILMTESTTRACK",
	"price": 453,
	"rid": "ab4219087a764ae0btest",
	"name": "Mascaras",
	"sale": 30,
	"size": "0",
	"total_price": 317,
	"nm_id": 6669212,
	"brand": "Boar Lui",
	"status": 202
	}
	],
	"locale": "en",
	"internal_signature": "",
	"customer_id": "test",
	"delivery_service": "meest",
	"shardkey": "9",
	"sm_id": 99,
	"date_created": "2021-11-26T06:22:19Z",
	"oof_shard": "1"
	}`

	defer conn.Close(context.Background())
	var order OrderInfo
	json.Unmarshal([]byte(json_value), &order)
	//InsertData(conn, order)
	res, err := GetDataByUid(conn, "b563feb7b2b84b6test")
	fmt.Println(res)
	//GetOrders(conn, "b563feb7b2b84b6test")
	//result := getItemsFromDb(conn, "WBILMTESTTRACK")
	//fmt.Println(result)
}

// InsertData Парсинг json и вставка данных в бд
func InsertData(conn *pgx.Conn, order OrderInfo) {
	err := InsertDataPayment(conn, order)
	fmt.Println(err)
	uid, err := InsertDataOrder(conn, order)
	fmt.Println(uid, err)
	id, err := InsertDataDelivery(conn, order)
	fmt.Println(id, err)
	err = InsertOrderDelivery(conn, uid, id)
	fmt.Println(err)
	err = InsertDataItems(conn, order)
}

func InsertDataOrder(conn *pgx.Conn, order OrderInfo) (order_uid string, err error) {
	query := `
		insert into order_info
			(order_uid, track_number, entry, locale, internal_signature,customer_id, delivery_service, shardkey, sm_id, date_created,oof_shard) 
		values 
			($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		returning order_uid
		`
	if err = conn.QueryRow(context.Background(), query,
		order.OrderUid,
		order.TrackNumber,
		order.Entry,
		order.Locale,
		order.InternalSignature,
		order.CustomerId,
		order.DeliveryService,
		order.Shardkey,
		order.SmId,
		order.DateCreated,
		order.OofShard,
	).Scan(&order_uid); err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			return "", pgErr
		}
	}
	return order_uid, nil
}

func InsertDataDelivery(conn *pgx.Conn, order OrderInfo) (id int, err error) {
	query := `
		insert into deliveries
			(name, phone, zip, city, address, region, email) 
		values 
			($1,$2,$3,$4,$5,$6,$7)
		returning id
		`
	if err = conn.QueryRow(context.Background(), query,
		order.Delivery.Name,
		order.Delivery.Phone,
		order.Delivery.Zip,
		order.Delivery.City,
		order.Delivery.Address,
		order.Delivery.Region,
		order.Delivery.Email,
	).Scan(&id); err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			fmt.Println(pgErr)
			return 0, pgErr
		}
	}
	fmt.Println(id)
	return id, nil
}
func InsertDataPayment(conn *pgx.Conn, order OrderInfo) (err error) {
	query := `
		insert into payments
			(transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total,
                      custom_fee) 
		values 
			($1,$2,$3,$4,$5,$6,$7, $8, $9, $10)
		`
	if err = conn.QueryRow(context.Background(), query,
		order.Payment.Transaction,
		order.Payment.RequestId,
		order.Payment.Currency,
		order.Payment.Provider,
		order.Payment.Amount,
		order.Payment.PaymentDt,
		order.Payment.Bank,
		order.Payment.DeliveryCost,
		order.Payment.GoodsTotal,
		order.Payment.CustomFee,
	).Scan(); err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			fmt.Println(pgErr)
			return pgErr
		}
	}
	return nil
}
func InsertOrderDelivery(conn *pgx.Conn, order_uid string, id int) (err error) {
	query := `
		insert into order_delivery
			(order_uid,delivery_id)
		values 
			($1,$2)
		`
	if err = conn.QueryRow(context.Background(), query, order_uid, id).Scan(); err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			fmt.Println(pgErr)
			return pgErr
		}
	}
	return nil
}
func InsertDataItems(conn *pgx.Conn, order OrderInfo) (err error) {
	for i := 0; i < len(order.Items); i++ {
		fmt.Println(order.Items[i])
		query := `
			insert into items
				(chrt_id,track_number,price,rid,name,sale,size,total_price,nm_id,brand,status)
			values
				($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			`
		if err = conn.QueryRow(context.Background(), query,
			order.Items[i].ChrtId,
			order.Items[i].TrackNumber,
			order.Items[i].Price,
			order.Items[i].Rid,
			order.Items[i].Name,
			order.Items[i].Sale,
			order.Items[i].Size,
			order.Items[i].TotalPrice,
			order.Items[i].NmId,
			order.Items[i].Brand,
			order.Items[i].Status,
		).Scan(); err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				fmt.Println(pgErr)
				return pgErr
			}
		}
	}

	return nil
}
func GetDataByUid(conn *pgx.Conn, order_uid string) (string, error) {
	var order OrderInfo
	query := `
		select oi.*, to_jsonb(p.*) as "payment", (select jsonb_agg((to_jsonb(i.*))) from items i )  as "items",
			to_jsonb(del) as "delivery" 
		from order_info oi 
		left join payments p on p."transaction" = oi.order_uid 
		left join items i on i.track_number = oi.track_number
		join (
				select d.name,d.phone ,d.zip ,d.city ,d.address ,d.region ,d.email  
				from deliveries d
				where d.id = (
								select od.delivery_id 
								from order_delivery od 
								where od.order_uid=$1
								)
			) as del on true
		where oi.order_uid = $1
		limit 1`
	if err := conn.QueryRow(context.Background(), query, order_uid).Scan(
		&order.OrderUid,
		&order.TrackNumber,
		&order.Entry,
		&order.Locale,
		&order.InternalSignature,
		&order.CustomerId,
		&order.DeliveryService,
		&order.Shardkey,
		&order.SmId,
		&order.DateCreated,
		&order.OofShard,
		&order.Payment,
		&order.Items,
		&order.Delivery,
	); err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			fmt.Println(pgErr)
			return "", pgErr
		}
	}
	res, _ := json.Marshal(&order)
	return string(res), nil
}

//
//func GetDataOrders(conn *pgx.Conn, order_uid string) {
//	var o OrderInfo
//	err := conn.QueryRow(context.Background(), "select to_json(*) from order_info where order_uid=$1", order_uid).Scan(
//		&o.OrderUid,
//		&o.TrackNumber,
//		&o.Entry,
//		&o.Locale,
//		&o.InternalSignature,
//		&o.CustomerId,
//		&o.DeliveryService,
//		&o.Shardkey,
//		&o.SmId,
//		&o.DateCreated,
//		&o.OofShard,
//	)
//	if err != nil {
//		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
//	}
//	result, err := json.Marshal(o)
//	fmt.Println(string(result))
//
//}
//
//func GetItems(conn *pgx.Conn, track_number string) string {
//
//	rows, err := conn.Query(context.Background(), "select * from items where track_number=$1", track_number)
//	if err != nil {
//		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
//	}
//	var rowSlice []Items
//
//	for rows.Next() {
//		var r Items
//		err := rows.Scan(&r.ChrtId, &r.TrackNumber, &r.Price, &r.Rid, &r.Name, &r.Sale, &r.Size, &r.TotalPrice, &r.NmId, &r.Brand, &r.Status)
//		if err != nil {
//			log.Fatal(err)
//		}
//		rowSlice = append(rowSlice, r)
//	}
//	if err := rows.Err(); err != nil {
//		log.Fatal(err)
//	}
//	result, err := json.Marshal(rowSlice)
//	return string(result)
//}
//
//func GetPayments(conn *pgx.Conn, order_uid string) {
//
//}
//
//func GetDeliveryId(conn *pgx.Conn, order_uid string) int {
//	return 0
//}
//
//func GetDataDelivery(conn *pgx.Conn, delivery_id int) {
//
//}
