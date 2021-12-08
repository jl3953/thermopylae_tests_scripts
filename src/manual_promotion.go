package main

import (
	"context"
	"flag"
	"google.golang.org/grpc"
	"log"
	"math"
	smdbrpc "smdbrpc/go/build/gen"
	"sort"
	"strings"
	"sync"
	"time"
)

func reverse(numbers []int) []int {
	for i := 0; i < len(numbers)/2; i++ {
		j := len(numbers) - i - 1
		numbers[i], numbers[j] = numbers[j], numbers[i]
	}
	return numbers
}

func dec2baseN(decimal int, baseN int) (digits []int) {
	for decimal > 0 {
		digit := decimal % baseN
		digits = append(digits, digit)
		decimal /= baseN
	}
	digits = reverse(digits)
	return digits
}

func convertToBase256(decimal int) (digits []int) {
	return dec2baseN(decimal, 256)
}

func encodeToCRDB(key int) (encoding []byte) {
	encoding = append(encoding, byte(189), byte(137))
	if key < 110 {
		encoding = append(encoding, byte(136+key))
	} else {
		digits := convertToBase256(key)
		encoding = append(encoding, byte(245+len(digits)))
		for _, digit := range digits {
			encoding = append(encoding, byte(digit))
		}
	}
	encoding = append(encoding, byte(136))
	return encoding
}

func promoteKeysToCicada(keys []int, walltime int64, logical int32,
	client smdbrpc.HotshardGatewayClient) {

	request := smdbrpc.PromoteKeysToCicadaReq{
		Keys: make([]*smdbrpc.Key, len(keys)),
	}
	for i, key := range keys {
		var table, index int64 = 53, 1
		keyCols := []int64{int64(key)}
		keyBytes := encodeToCRDB(key)
		jennifer := []int{82, 196, 81, 94, 10, 38, 8, 106, 101, 110, 110, 105,
			102, 101, 114} // 4-byte checksum, 10, 38, valLen=8, jennifer
		valBytes := make([]byte, len(jennifer))
		for j, b := range jennifer {
			valBytes[j] = byte(b)
		}

		request.Keys[i] = &smdbrpc.Key{
			Table:   &table,
			Index:   &index,
			KeyCols: keyCols,
			Key:     keyBytes,
			Timestamp: &smdbrpc.HLCTimestamp{
				Walltime:    &walltime,
				Logicaltime: &logical,
			},
			Value: valBytes,
		}
	}

	sort.Slice(request.Keys, func(i, j int) bool {
		return request.Keys[i].KeyCols[0] < request.Keys[i].KeyCols[0]
	})

	// promote to cicada
	reply, err := client.PromoteKeysToCicada(context.Background(), &request)
	if err != nil {
		log.Fatalf("Failed to send, err %+v\n", err)
	} else {
		for _, didKeySucceed := range reply.GetSuccessfullyPromoted() {
			if !didKeySucceed {
				log.Fatalf("Key did not get promoted\n")
			}
		}
	}
}

func updateCRDBPromotionMaps(keys []int, walltime int64, logical int32,
	clients []smdbrpc.HotshardGatewayClient) {

	// populate promotion request
	updateMapReq := smdbrpc.PromoteKeysReq{
		Keys: make([]*smdbrpc.KVVersion, len(keys)),
	}
	for i, key := range keys {
		updateMapReq.Keys[i] = &smdbrpc.KVVersion{
			Key:       encodeToCRDB(key),
			Value:     nil,
			Timestamp: &smdbrpc.HLCTimestamp{
				Walltime:    &walltime,
				Logicaltime: &logical,
			},
			Hotness:   nil,
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < len(clients); i++ {
		wg.Add(1)
		go func(clientIdx int) {
			defer wg.Done()
			client := clients[clientIdx]
			crdbCtx, crdbCancel := context.WithTimeout(context.Background(),
				time.Second)
			defer crdbCancel()

			resp, err := client.UpdatePromotionMap(crdbCtx, &updateMapReq)
			if err != nil {
				log.Fatalf("cannot send updatePromoMapReq CRDB node %d" +
					"err %+v\n", clientIdx, err)
			}

			for _, keyMigrationResp := range resp.WereSuccessfullyMigrated {
				if !keyMigrationResp.GetIsSuccessfullyMigrated() {
					log.Fatalf("did not update all keys in map CRDB node %d",
						clientIdx)
				}
			}

		}(i)
	}
	wg.Wait()
}

type Wrapper struct {
	Addr string
	ConnPtr *grpc.ClientConn
	Client smdbrpc.HotshardGatewayClient
}

func grpcConnect(wrapper *Wrapper) {
	var err error
	wrapper.ConnPtr, err = grpc.Dial(wrapper.Addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial %+v\n", wrapper.Addr)
	}
	wrapper.Client = smdbrpc.NewHotshardGatewayClient(wrapper.ConnPtr)
}

func promoteKeys(keys []int, batch int, walltime int64, logical int32,
	cicadaAddr string, crdbAddresses []string) {

	// connect to Cicada
	cicadaWrapper := Wrapper{
		Addr:    cicadaAddr,
	}
	grpcConnect(&cicadaWrapper)

	// connect to CRDB
	crdbWrappers := make([]Wrapper, len(crdbAddresses))
	for i, crdbAddr := range crdbAddresses {
		crdbWrappers[i] = Wrapper{
			Addr:    crdbAddr,
		}
		grpcConnect(&crdbWrappers[i])
	}
	crdbClients := make([]smdbrpc.HotshardGatewayClient, len(crdbWrappers))
	for i, wrapper := range crdbWrappers {
		crdbClients[i] = wrapper.Client
	}

	// promote keys in batches
	for i := 0; i < len(keys); i += batch {
		max := math.Min(float64(i+batch), float64(len(keys)))
		promoteKeysToCicada(keys[i:int(max)], walltime, logical,
			cicadaWrapper.Client)
		updateCRDBPromotionMaps(keys[i:int(max)], walltime, logical,
			crdbClients)
	}
}

func main() {
	batch := flag.Int("batch", 1,
		"number of keys to promote in a single batch")
	cicadaAddr := flag.String("cicadaAddr", "node-11:50051",
		"cicada host machine")
	crdbAddrs := flag.String("crdbAddrs", "node-8:50055,node-9:50055",
		"csv of crdb addresses")
	keyMin := flag.Int("keyMin", 0, "minimum key to promote")
	keyMax := flag.Int("keyMax", 0, "one over the maximum key to promote")
	flag.Parse()

	crdbAddrsSlice := strings.Split(*crdbAddrs, ",")

	log.Printf("batch %d, cicadaAddr %s, crdbAddrs %+s\n", *batch, *cicadaAddr,
		crdbAddrsSlice)

	walltime := time.Now().UnixNano()
	var logical int32 = 0

	if *keyMax - *keyMin > 0 {
		keys := make([]int, *keyMax-*keyMin)
		for i := *keyMin; i < *keyMax; i++ {
			keys[i] = i
		}
		promoteKeys(keys, *batch, walltime, logical, *cicadaAddr, crdbAddrsSlice)
	}
}
