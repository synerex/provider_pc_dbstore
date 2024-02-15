package main

import (
	// "encoding/json"
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	protoPC "github.com/synerex/proto_pcounter"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/jackc/pgx/v4/pgxpool"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"

	sxutil "github.com/synerex/synerex_sxutil"
	//sxutil "local.packages/synerex_sxutil"

	"log"
	"sync"
)

// datastore provider provides Datastore Service.

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local           = flag.String("local", "", "Local Synerex Server")
	mu              sync.Mutex
	version         = "0.01"
	baseDir         = "store"
	dataDir         string
	pcMu            *sync.Mutex = nil
	pcLoop          *bool       = nil
	ssMu            *sync.Mutex = nil
	ssLoop          *bool       = nil
	sxServerAddress string
	currentNid      uint64                  = 0 // NotifyDemand message ID
	mbusID          uint64                  = 0 // storage MBus ID
	storageID       uint64                  = 0 // storageID
	pcClient        *sxutil.SXServiceClient = nil
	db              *pgxpool.Pool
	db_host         = os.Getenv("POSTGRES_HOST")
	db_name         = os.Getenv("POSTGRES_DB")
	db_user         = os.Getenv("POSTGRES_USER")
	db_pswd         = os.Getenv("POSTGRES_PASSWORD")
)

const layout = "2006-01-02T15:04:05.999999Z"
const layout_db = "2006-01-02 15:04:05.999"

func init() {
	// connect
	ctx := context.Background()
	addr := fmt.Sprintf("postgres://%s:%s@%s:5432/%s", db_user, db_pswd, db_host, db_name)
	print("connecting to " + addr + "\n")
	var err error
	db, err = pgxpool.Connect(ctx, addr)
	if err != nil {
		print("connection error: ")
		log.Println(err)
		log.Fatal("\n")
	}
	defer db.Close()

	// ping
	err = db.Ping(ctx)
	if err != nil {
		print("ping error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	// create table
	_, err = db.Exec(ctx, `create table if not exists pc(id BIGSERIAL NOT NULL, time TIMESTAMP not null, mac BIGINT not null, hostname VARCHAR(24) not null, sid INT not null, dir CHAR(2) not null, height INT not null, constraint pc_pk unique (time, sid, dir, height))`)
	// select hex(mac) from log;
	// insert into pc (mac) values (x'000CF15698AD');
	if err != nil {
		print("create table error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	_, err = db.Exec(ctx, `SELECT create_hypertable('pc', 'time', migrate_data => true, if_not_exists => true, chunk_time_interval => INTERVAL '1 week')`)
	if err != nil {
		print("create_hypertable error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	_, err = db.Exec(ctx, `SELECT attach_tablespace('fast_space', 'pc', if_not_attached => true)`)
	if err != nil {
		print("attach_tablespace error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	_, err = db.Exec(ctx, `CLUSTER pc USING pc_time_idx`)
	if err != nil {
		print("CLUSTER error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	_, err = db.Exec(ctx, `ALTER TABLE pc SET (timescaledb.compress, timescaledb.compress_segmentby = 'sid', timescaledb.compress_orderby = 'time, dir, height DESC')`)
	if err != nil {
		print("ALTER TABLE pc SET error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	var count int
	if err := db.QueryRow(ctx, `select count(*) from timescaledb_information.jobs where proc_name like 'move_old_chunks' and config?'hypertable' and config->>'hypertable' like 'pc'`).Scan(&count); err != nil {
		print("select count(*) from timescaledb_information.jobs query error: ")
		log.Println(err)
		print("\n")
	} else {
		if count == 0 {
			_, err = db.Exec(ctx, `SELECT add_job('move_old_chunks', '1 week', config => '{"hypertable":"pc","lag":"1 year","tablespace":"slow_space"}')`)
			if err != nil {
				print("SELECT add_job: ")
				log.Println(err)
				log.Fatal("\n")
			}
		}
	}
}

func dbStore(ts time.Time, mac string, hostname string, sid uint32, dir string, height uint32) {

	// ping
	ctx := context.Background()
	err := db.Ping(ctx)
	if err != nil {
		print("ping error: ")
		log.Println(err)
		print("\n")
		// connect
		addr := fmt.Sprintf("postgres://%s:%s@%s:5432/%s", db_user, db_pswd, db_host, db_name)
		print("connecting to " + addr + "\n")
		db, err = pgxpool.Connect(ctx, addr)
		if err != nil {
			print("connection error: ")
			log.Println(err)
			print("\n")
		}
	}

	hexmac := strings.Replace(mac, ":", "", -1)
	nummac, err := strconv.ParseUint(hexmac, 16, 64)
	if err != nil {
		panic(err)
	}

	// log.Printf("Storeing %v, %s, %s, %d, %s, %d", ts.Format(layout_db), hexmac, hostname, sid, dir, height)
	result, err := db.Exec(ctx, `insert into pc(time, mac, hostname, sid, dir, height) values($1, $2, $3, $4, $5, $6)`, ts.Format(layout_db), nummac, hostname, sid, dir, height)

	if err != nil {
		print("exec error: ")
		log.Println(err)
		print("\n")
	} else {
		rowsAffected := result.RowsAffected()
		if err != nil {
			log.Println(err)
		} else {
			print(rowsAffected)
		}
	}

}

// called for each agent data.
func supplyPCountCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {

	if sp.SupplyName == "PCounter" {
		pc := &protoPC.PCounter{}
		err := proto.Unmarshal(sp.Cdata.Entity, pc)
		if err == nil {
			sliceHostname := strings.Split(pc.Hostname, "-vc3d-")
			sid, _ := strconv.Atoi(sliceHostname[0])
			sid += 1000 // Offset for centrair, pc sensors
			for _, v := range pc.Data {
				if v.Typ == "counter" && v.Id == "1" {
					ts, _ := time.Parse(layout, ptypes.TimestampString(v.Ts))
					dbStore(ts, pc.Mac, pc.Hostname, uint32(sid), v.Dir, v.Height)
				}
			}
		} else {
			log.Printf("Unmarshaling err PC: %v", err)
		}
	} else {
		log.Printf("Received Unknown (%4d bytes)", len(sp.Cdata.Entity))
	}
}

func main() {
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)
	log.Printf("PC-dbstore(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)

	channelTypes := []uint32{pbase.PEOPLE_WT_SVC, pbase.STORAGE_SERVICE}

	var rerr error
	sxServerAddress, rerr = sxutil.RegisterNode(*nodesrv, "PCdbstore", channelTypes, nil)

	if rerr != nil {
		log.Fatal("Can't register node:", rerr)
	}
	if *local != "" { // quick hack for AWS local network
		sxServerAddress = *local
	}
	log.Printf("Connecting SynerexServer at [%s]", sxServerAddress)

	wg := sync.WaitGroup{} // for syncing other goroutines

	client := sxutil.GrpcConnectServer(sxServerAddress)

	if client == nil {
		log.Fatal("Can't connect Synerex Server")
	}

	pcClient = sxutil.NewSXServiceClient(client, pbase.PEOPLE_COUNTER_SVC, "{Client:PCdbStore}")

	log.Print("Subscribe PCount Supply")
	pcMu, pcLoop = sxutil.SimpleSubscribeSupply(pcClient, supplyPCountCallback)

	wg.Add(1)
	wg.Wait()
}
