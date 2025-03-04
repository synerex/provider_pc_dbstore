package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/jackc/pgx/v4/pgxpool"
	protoPC "github.com/synerex/proto_pcounter"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local           = flag.String("local", "", "Local Synerex Server")
	sxServerAddress string
	pcClient        *sxutil.SXServiceClient
	db              *pgxpool.Pool
	// 環境変数などから取得していると仮定
	db_host = os.Getenv("POSTGRES_HOST")
	db_name = os.Getenv("POSTGRES_DB")
	db_user = os.Getenv("POSTGRES_USER")
	db_pswd = os.Getenv("POSTGRES_PASSWORD")
)

const layout = "2006-01-02T15:04:05.999999Z"
const layout_db = "2006-01-02 15:04:05.999"

// findSidFromHostnameAndTime は hostname, time から data_source_hardwares テーブルを検索し、
// 該当する sid を返す。該当レコードが無い場合はエラーを返すので、呼び出し側でフォールバックする。
func findSidFromHostnameAndTime(hostname string, t time.Time) (int, error) {
	ctx := context.Background()
	var sid int
	// ここで hostname に一致し、かつ t が operation_start_time <= t < operation_end_time の範囲にあるレコードを探す
	sql := `
		SELECT sid
		FROM data_source_hardwares
		WHERE hostname = $1
		  AND operation_start_time <= $2
		  AND operation_end_time >  $2
		LIMIT 1
	`
	err := db.QueryRow(ctx, sql, hostname, t).Scan(&sid)
	if err != nil {
		return 0, err
	}
	return sid, nil
}

func dbStore(ts time.Time, mac string, hostname string, sid uint32, dir string, height uint32) {
	// DB へ INSERT する部分
	// (pgxpool での接続確認などは省略)
	ctx := context.Background()

	// MAC を 16進→10進に変換
	hexmac := strings.Replace(mac, ":", "", -1)
	nummac, err := strconv.ParseUint(hexmac, 16, 64)
	if err != nil {
		panic(err)
	}

	result, err := db.Exec(ctx,
		`INSERT INTO pc(time, mac, hostname, sid, dir, height)
		 VALUES($1, $2, $3, $4, $5, $6)`,
		ts.Format(layout_db), nummac, hostname, sid, dir, height)

	if err != nil {
		log.Printf("exec error: %v\n", err)
		return
	}
	rowsAffected := result.RowsAffected()
	log.Printf("Rows affected: %d\n", rowsAffected)
}

func supplyPCountCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	if sp.SupplyName == "PCounter" {
		pc := &protoPC.PCounter{}
		err := proto.Unmarshal(sp.Cdata.Entity, pc)
		if err == nil {
			for _, v := range pc.Data {
				if v.Typ == "counter" && v.Id == "1" {
					// v.Ts は ptypes.TimestampString() で time.Time に変換
					ts, _ := time.Parse(layout, ptypes.TimestampString(v.Ts))

					// まずは新方式で sid を取得してみる
					dbSid, sidErr := findSidFromHostnameAndTime(pc.Hostname, ts)

					// もし該当レコードが無い/エラーなら、従来のフォールバック方式
					if sidErr != nil {
						sliceHostname := strings.Split(pc.Hostname, "-vc3d-")
						fallbackSid, _ := strconv.Atoi(sliceHostname[0])
						fallbackSid += 1000 // Offset
						dbSid = fallbackSid
					}

					// DB へ書き込み
					dbStore(ts, pc.Mac, pc.Hostname, uint32(dbSid), v.Dir, v.Height)
				}
			}
		} else {
			log.Printf("Unmarshaling err PC: %v", err)
		}
	} else {
		log.Printf("Received Unknown supply (%4d bytes)", len(sp.Cdata.Entity))
	}
}

func main() {
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)
	log.Printf("PC-dbstore(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)

	// DB 接続
	ctx := context.Background()
	addr := fmt.Sprintf("postgres://%s:%s@%s:5432/%s", db_user, db_pswd, db_host, db_name)
	var err error
	db, err = pgxpool.Connect(ctx, addr)
	if err != nil {
		log.Fatalf("connection error: %v\n", err)
	}
	defer db.Close()

	// Synerex への接続など
	sxServerAddress, rerr := sxutil.RegisterNode(*nodesrv, "PCdbstore", []uint32{pbase.PEOPLE_WT_SVC, pbase.STORAGE_SERVICE}, nil)
	if rerr != nil {
		log.Fatal("Can't register node:", rerr)
	}
	if *local != "" { // quick hack for AWS local network
		sxServerAddress = *local
	}
	log.Printf("Connecting SynerexServer at [%s]", sxServerAddress)

	client := sxutil.GrpcConnectServer(sxServerAddress)

	if client == nil {
		log.Fatal("Can't connect Synerex Server")
	}

	pcClient = sxutil.NewSXServiceClient(client, pbase.PEOPLE_COUNTER_SVC, "{Client:PCdbStore}")

	log.Print("Subscribe PCount Supply")
	// サブスクライブしてデータが来たら supplyPCountCallback が呼ばれる
	_, _ = sxutil.SimpleSubscribeSupply(pcClient, supplyPCountCallback)

	// メインスレッドが終了しないように
	select {}
}
