package pika_integration

import (
	"context"
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("Replication", func() {
	var (
		ctx          = context.TODO()
		master       *redis.Client
		slave        *redis.Client
		delaySlaveOf = false
		mu           sync.Mutex
	)

	BeforeEach(func() {
		master = redis.NewClient(pikaOptions1())
		slave = redis.NewClient(pikaOptions2())

		Expect(master.FlushAll(ctx).Err()).NotTo(HaveOccurred())
		Expect(slave.SlaveOf(ctx, "no", "one").Err()).NotTo(HaveOccurred())
		Expect(slave.FlushAll(ctx).Err()).NotTo(HaveOccurred())
		time.Sleep(5 * time.Second)

		if !delaySlaveOf {
			Expect(slave.SlaveOf(ctx, masterIP, masterPort).Err()).NotTo(HaveOccurred())
		}
	})

	AfterEach(func() {
		Expect(master.Close()).NotTo(HaveOccurred())
		Expect(slave.Close()).NotTo(HaveOccurred())
	})

	//delayReplication := func() {
	//	if delaySlaveOf {
	//		Expect(slave.SlaveOf(ctx, masterIP, masterPort).Err()).NotTo(HaveOccurred())
	//	}
	//	// wait for replication
	//	time.Sleep(20 * time.Second)
	//}

	Describe("After executing multiple keys", func() {
		Context("perform master-slave replication", func() {
			It("should stay in sync", func() {
				mu.Lock()
				defer mu.Unlock()
				master.RPush(ctx, "blist3", "v2")
				master.LPush(ctx, "blist2", "v2")
				master.LPop(ctx, "blist3")
				master.RPop(ctx, "blist2")

				master.LPush(ctx, "blist2", "v2")
				master.LPop(ctx, "blist2")
				master.RPush(ctx, "blist3", "v2")
				master.RPop(ctx, "blist3")

				master.LPush(ctx, "blist2", "v2")
				master.LPop(ctx, "blist2")
				master.RPush(ctx, "blist3", "v2")
				master.LPush(ctx, "blist2", "v2")

				master.RPop(ctx, "blist3")
				master.LPop(ctx, "blist2")
				master.RPush(ctx, "blist3", "v2")
				master.LPush(ctx, "blist2", "v2")

				master.RPop(ctx, "blist3")
				master.RPush(ctx, "blist3", "v2")
				master.LPush(ctx, "blist2", "v2")
				master.RPush(ctx, "blist3", "v2")

				master.RPush(ctx, "blist3", "v2")
				master.LPush(ctx, "blist2", "v2")
				master.LPush(ctx, "blist2", "v2")
				master.RPush(ctx, "blist3", "v2")

				master.LPush(ctx, "blist2", "v2")
				master.RPush(ctx, "blist3", "v2")

				master.Del(ctx, "blist1", "large", "blist2")

				master.RPush(ctx, "blist1", "a", "large", "c")
				master.RPush(ctx, "blist2", "d", "large", "f")

				master.LPop(ctx, "blist1")
				master.RPop(ctx, "blist1")
				master.LPop(ctx, "blist2")
				master.RPop(ctx, "blist2")

				master.Del(ctx, "blist3")
				master.LPop(ctx, "blist2")
				master.RPop(ctx, "blist1")

				if delaySlaveOf {
					Expect(slave.SlaveOf(ctx, masterIP, masterPort).Err()).NotTo(HaveOccurred())
				}
				// wait for replication
				time.Sleep(20 * time.Second)

				mKeys := master.Keys(ctx, "*")
				sKeys := slave.Keys(ctx, "*")

				Expect(mKeys.Val()).To(ConsistOf(sKeys.Val()))

				lists_ := []string{"blist1", "blist2", "blist3"}
				for _, thisList := range lists_ {
					mListLen := master.LLen(ctx, thisList).Val()
					sListLen := slave.LLen(ctx, thisList).Val()
					Expect(mListLen).To(Equal(sListLen))

					for i := int64(0); i < mListLen; i++ {
						mVal := master.LIndex(ctx, thisList, i).Val()
						sVal := slave.LIndex(ctx, thisList, i).Val()
						Expect(mVal).To(Equal(sVal))
					}
				}
			})
		})
	})

	Describe("After executing MSetNX", func() {
		Context("perform master-slave replication", func() {
			It("should stay in sync", func() {
				randomMSetThread := func(keys []string) func(*sync.WaitGroup) {
					return func(wg *sync.WaitGroup) {
						mu.Lock()
						defer mu.Unlock()

						for i := 0; i < 3; i++ {
							kvs1 := make(map[string]interface{})
							kvs2 := make(map[string]interface{})
							kvs3 := make(map[string]interface{})
							kvs4 := make(map[string]interface{})

							for _, key := range keys {
								kvs1[key] = randStr(5)
								kvs2[key] = randStr(5)
								kvs3[key] = randStr(5)
								kvs4[key] = randStr(5)
							}
							master.Set(ctx, keys[2], randStr(5), 0)
							master.Set(ctx, keys[3], randStr(5), 0)
							master.Del(ctx, keys...)
							master.MSetNX(ctx, kvs1)
							master.Set(ctx, keys[0], randStr(5), 0)
							master.Set(ctx, keys[1], randStr(5), 0)
							master.Del(ctx, keys...)
							master.MSetNX(ctx, kvs2)
							master.Set(ctx, keys[1], randStr(5), 0)
							master.Set(ctx, keys[2], randStr(5), 0)
							master.Set(ctx, keys[0], randStr(5), 0)
							master.Del(ctx, keys...)
							master.MSetNX(ctx, kvs3)
							master.Set(ctx, keys[3], randStr(5), 0)
							master.Set(ctx, keys[0], randStr(5), 0)
							master.Set(ctx, keys[1], randStr(5), 0)
							master.Del(ctx, keys...)
							master.MSetNX(ctx, kvs4)
							master.Set(ctx, keys[3], randStr(5), 0)
						}
					}
				}

				mu.Lock()
				defer mu.Unlock()

				keys := []string{"1mset_key", "2mset_key", "3mset_key", "4mset_key"}
				threads := make([]*sync.WaitGroup, 50)
				for i := 0; i < 50; i++ {
					wg := &sync.WaitGroup{}
					wg.Add(1)
					threads[i] = wg

					go func() {
						randomMSetThread(keys)
						wg.Done()
					}()
				}

				for _, wg := range threads {
					wg.Wait()
				}

				if delaySlaveOf {
					Expect(slave.SlaveOf(ctx, masterIP, masterPort).Err()).NotTo(HaveOccurred())
				}
				// wait for replication
				time.Sleep(20 * time.Second)

				for _, key := range keys {
					mVal, err := master.Get(ctx, key).Result()
					Expect(err).NotTo(HaveOccurred())

					sVal, err := slave.Get(ctx, key).Result()
					Expect(err).NotTo(HaveOccurred())

					Expect(mVal).To(Equal(sVal))
				}
			})
		})
	})

	//Describe("Replication", func() {
	//	It("should multiple keys", func() {
	//
	//		mu.Lock()
	//		defer mu.Unlock()
	//		master.RPush(ctx, "blist3", "v2")
	//		master.LPush(ctx, "blist2", "v2")
	//		master.LPop(ctx, "blist3")
	//		master.RPop(ctx, "blist2")
	//
	//		master.LPush(ctx, "blist2", "v2")
	//		master.LPop(ctx, "blist2")
	//		master.RPush(ctx, "blist3", "v2")
	//		master.RPop(ctx, "blist3")
	//
	//		master.LPush(ctx, "blist2", "v2")
	//		master.LPop(ctx, "blist2")
	//		master.RPush(ctx, "blist3", "v2")
	//		master.LPush(ctx, "blist2", "v2")
	//
	//		master.RPop(ctx, "blist3")
	//		master.LPop(ctx, "blist2")
	//		master.RPush(ctx, "blist3", "v2")
	//		master.LPush(ctx, "blist2", "v2")
	//
	//		master.RPop(ctx, "blist3")
	//		master.RPush(ctx, "blist3", "v2")
	//		master.LPush(ctx, "blist2", "v2")
	//		master.RPush(ctx, "blist3", "v2")
	//
	//		master.RPush(ctx, "blist3", "v2")
	//		master.LPush(ctx, "blist2", "v2")
	//		master.LPush(ctx, "blist2", "v2")
	//		master.RPush(ctx, "blist3", "v2")
	//
	//		master.LPush(ctx, "blist2", "v2")
	//		master.RPush(ctx, "blist3", "v2")
	//
	//		master.Del(ctx, "blist1", "large", "blist2")
	//
	//		master.RPush(ctx, "blist1", "a", "large", "c")
	//		master.RPush(ctx, "blist2", "d", "large", "f")
	//
	//		master.LPop(ctx, "blist1")
	//		master.RPop(ctx, "blist1")
	//		master.LPop(ctx, "blist2")
	//		master.RPop(ctx, "blist2")
	//
	//		master.Del(ctx, "blist3")
	//		master.LPop(ctx, "blist2")
	//		master.RPop(ctx, "blist1")
	//
	//		delayReplication()
	//
	//		mKeys := master.Keys(ctx, "*")
	//		sKeys := slave.Keys(ctx, "*")
	//
	//		Expect(mKeys.Val()).To(ConsistOf(sKeys.Val()))
	//
	//		lists_ := []string{"blist1", "blist2", "blist3"}
	//		for _, thisList := range lists_ {
	//			mListLen := master.LLen(ctx, thisList).Val()
	//			sListLen := slave.LLen(ctx, thisList).Val()
	//			Expect(mListLen).To(Equal(sListLen))
	//
	//			for i := int64(0); i < mListLen; i++ {
	//				mVal := master.LIndex(ctx, thisList, i).Val()
	//				sVal := slave.LIndex(ctx, thisList, i).Val()
	//				Expect(mVal).To(Equal(sVal))
	//			}
	//		}
	//	})
	//
	//	randomMSetThread := func(keys []string) func(*sync.WaitGroup) {
	//		return func(wg *sync.WaitGroup) {
	//			mu.Lock()
	//			defer mu.Unlock()
	//
	//			for i := 0; i < 3; i++ {
	//				kvs1 := make(map[string]interface{})
	//				kvs2 := make(map[string]interface{})
	//				kvs3 := make(map[string]interface{})
	//				kvs4 := make(map[string]interface{})
	//
	//				for _, key := range keys {
	//					kvs1[key] = randStr(5)
	//					kvs2[key] = randStr(5)
	//					kvs3[key] = randStr(5)
	//					kvs4[key] = randStr(5)
	//				}
	//				master.Set(ctx, keys[2], randStr(5), 0)
	//				master.Set(ctx, keys[3], randStr(5), 0)
	//				master.Del(ctx, keys...)
	//				master.MSetNX(ctx, kvs1)
	//				master.Set(ctx, keys[0], randStr(5), 0)
	//				master.Set(ctx, keys[1], randStr(5), 0)
	//				master.Del(ctx, keys...)
	//				master.MSetNX(ctx, kvs2)
	//				master.Set(ctx, keys[1], randStr(5), 0)
	//				master.Set(ctx, keys[2], randStr(5), 0)
	//				master.Set(ctx, keys[0], randStr(5), 0)
	//				master.Del(ctx, keys...)
	//				master.MSetNX(ctx, kvs3)
	//				master.Set(ctx, keys[3], randStr(5), 0)
	//				master.Set(ctx, keys[0], randStr(5), 0)
	//				master.Set(ctx, keys[1], randStr(5), 0)
	//				master.Del(ctx, keys...)
	//				master.MSetNX(ctx, kvs4)
	//				master.Set(ctx, keys[3], randStr(5), 0)
	//			}
	//		}
	//	}
	//
	//	It("should MSetNX", func() {
	//		mu.Lock()
	//		defer mu.Unlock()
	//
	//		keys := []string{"1mset_key", "2mset_key", "3mset_key", "4mset_key"}
	//		threads := make([]*sync.WaitGroup, 50)
	//		for i := 0; i < 50; i++ {
	//			wg := &sync.WaitGroup{}
	//			wg.Add(1)
	//			threads[i] = wg
	//
	//			go func() {
	//				randomMSetThread(keys)
	//				wg.Done()
	//			}()
	//		}
	//
	//		for _, wg := range threads {
	//			wg.Wait()
	//		}
	//
	//		delayReplication()
	//
	//		for _, key := range keys {
	//			mVal, err := master.Get(ctx, key).Result()
	//			Expect(err).NotTo(HaveOccurred())
	//
	//			sVal, err := slave.Get(ctx, key).Result()
	//			Expect(err).NotTo(HaveOccurred())
	//
	//			Expect(mVal).To(Equal(sVal))
	//		}
	//	})
	//
	//	It("should MSet", func() {
	//		mu.Lock()
	//		defer mu.Unlock()
	//
	//		keys := []string{"1mset_key", "2mset_key", "3mset_key", "4mset_key"}
	//		master.Del(ctx, keys...)
	//
	//		var wg sync.WaitGroup
	//		for i := 0; i < 50; i++ {
	//			wg.Add(1)
	//
	//			go func() {
	//				randomMSetThread(keys)
	//				wg.Done()
	//			}()
	//		}
	//		wg.Wait()
	//
	//		delayReplication()
	//
	//		for _, key := range keys {
	//			mVal, err := master.Get(ctx, key).Result()
	//			Expect(err).NotTo(HaveOccurred())
	//			sVal, err := slave.Get(ctx, key).Result()
	//			Expect(err).NotTo(HaveOccurred())
	//			Expect(mVal).To(Equal(sVal))
	//		}
	//	})
	//
	//	It("should SMove", func() {
	//		mu.Lock()
	//		defer mu.Unlock()
	//
	//		randomSMoveThread := func() {
	//			for i := 0; i < 1; i++ {
	//				member := randStr(5)
	//				master.SAdd(ctx, "source_set", member, member, member)
	//				master.SRem(ctx, "dest_set", member, member)
	//				master.SMove(ctx, "source_set", "dest_set", member)
	//			}
	//		}
	//
	//		threads := make([]*sync.WaitGroup, 10)
	//		for i := 0; i < 10; i++ {
	//			wg := &sync.WaitGroup{}
	//			wg.Add(1)
	//			threads[i] = wg
	//
	//			go func() {
	//				randomSMoveThread()
	//				wg.Done()
	//			}()
	//		}
	//
	//		for _, wg := range threads {
	//			wg.Wait()
	//		}
	//
	//		delayReplication()
	//
	//		mSourceSet := master.SMembers(ctx, "source_set").Val()
	//		mDestSet := master.SMembers(ctx, "dest_set").Val()
	//		sSourceSet := slave.SMembers(ctx, "source_set").Val()
	//		sDestSet := slave.SMembers(ctx, "dest_set").Val()
	//
	//		Expect(mSourceSet).To(Equal(sSourceSet))
	//		Expect(mDestSet).To(Equal(sDestSet))
	//	})
	//
	//	It("should RPopLPush", func() {
	//		mu.Lock()
	//		defer mu.Unlock()
	//
	//		rpoplpushThread := func() {
	//			for i := 0; i < 50; i++ {
	//				randomStr1 := randStr(5)
	//				randomStr2 := randStr(5)
	//				randomStr3 := randStr(5)
	//				master.LPush(ctx, "blist0", randomStr1)
	//				master.RPopLPush(ctx, "blist0", "blist")
	//				master.LPush(ctx, "blist", randomStr1, randomStr2, randomStr3)
	//
	//				master.LPop(ctx, "blist")
	//				master.RPop(ctx, "blist")
	//				master.LPush(ctx, "blist0", randomStr3)
	//				master.RPopLPush(ctx, "blist0", "blist")
	//				master.RPush(ctx, "blist", randomStr3, randomStr2, randomStr1)
	//				master.LPop(ctx, "blist")
	//				master.LPush(ctx, "blist0", randomStr2)
	//				master.RPopLPush(ctx, "blist0", "blist")
	//				master.RPop(ctx, "blist")
	//			}
	//		}
	//
	//		threads := make([]*sync.WaitGroup, 10)
	//		for i := 0; i < 10; i++ {
	//			wg := &sync.WaitGroup{}
	//			wg.Add(1)
	//			threads[i] = wg
	//
	//			go func() {
	//				rpoplpushThread()
	//				wg.Done()
	//			}()
	//		}
	//
	//		for _, wg := range threads {
	//			wg.Wait()
	//		}
	//
	//		delayReplication()
	//
	//		mKeys := master.Keys(ctx, "*").Val()
	//		sKeys := slave.Keys(ctx, "*").Val()
	//
	//		Expect(sKeys).To(Equal(mKeys))
	//
	//		mBlistLen := master.LLen(ctx, "blist").Val()
	//		sBlistLen := slave.LLen(ctx, "blist").Val()
	//		Expect(sBlistLen).To(Equal(mBlistLen))
	//
	//		for i := int64(0); i < mBlistLen; i++ {
	//			mVal := master.LIndex(ctx, "blist", i).Val()
	//			sVal := slave.LIndex(ctx, "blist", i).Val()
	//			Expect(sVal).To(Equal(mVal))
	//		}
	//	})
	//
	//	It("should SDiffStore", func() {
	//		mu.Lock()
	//		defer mu.Unlock()
	//
	//		sdiffstoreThread := func() {
	//			for i := 0; i < 10; i++ {
	//				master.SAdd(ctx, "set1", randStr(5))
	//				master.SAdd(ctx, "set2", randStr(5))
	//				master.SAdd(ctx, "set1", randStr(5))
	//				master.SAdd(ctx, "set2", randStr(5))
	//				master.SAdd(ctx, "set1", randStr(5))
	//				master.SAdd(ctx, "set2", randStr(5))
	//				master.SAdd(ctx, "set1", randStr(5))
	//				master.SAdd(ctx, "set2", randStr(5))
	//				master.SAdd(ctx, "set2", randStr(5))
	//				master.SAdd(ctx, "set1", randStr(5))
	//				master.SAdd(ctx, "set1", randStr(5))
	//				master.SAdd(ctx, "set1", randStr(5))
	//				master.SAdd(ctx, "set2", randStr(5))
	//				master.SAdd(ctx, "set2", randStr(5))
	//				master.SAdd(ctx, "set1", randStr(5))
	//				master.SAdd(ctx, "set2", randStr(5))
	//				master.SAdd(ctx, "dest_set", randStr(5))
	//				master.SDiffStore(ctx, "dest_set", "set1", "set2")
	//			}
	//		}
	//
	//		threads := make([]*sync.WaitGroup, 10)
	//		for i := 0; i < 10; i++ {
	//			wg := &sync.WaitGroup{}
	//			wg.Add(1)
	//			threads[i] = wg
	//
	//			go func() {
	//				sdiffstoreThread()
	//				wg.Done()
	//			}()
	//		}
	//
	//		for _, wg := range threads {
	//			wg.Wait()
	//		}
	//
	//		delayReplication()
	//
	//		mSet1 := master.SMembers(ctx, "set1").Val()
	//		mSet2 := master.SMembers(ctx, "set2").Val()
	//		mDestSet := master.SMembers(ctx, "dest_set").Val()
	//		sSet1 := slave.SMembers(ctx, "set1").Val()
	//		sSet2 := slave.SMembers(ctx, "set2").Val()
	//		sDestSet := slave.SMembers(ctx, "dest_set").Val()
	//
	//		Expect(sSet1).To(Equal(mSet1))
	//		Expect(sSet2).To(Equal(mSet2))
	//		Expect(sDestSet).To(Equal(mDestSet))
	//	})
	//
	//	It("should SInterStore", func() {
	//		mu.Lock()
	//		defer mu.Unlock()
	//
	//		sinterstoreThread := func() {
	//			for i := 0; i < 10; i++ {
	//				member := randStr(5)
	//				member2 := randStr(5)
	//				member3 := randStr(5)
	//				member4 := randStr(5)
	//				member5 := randStr(5)
	//				member6 := randStr(5)
	//				master.SAdd(ctx, "set1", member)
	//				master.SAdd(ctx, "set2", member)
	//				master.SAdd(ctx, "set1", member2)
	//				master.SAdd(ctx, "set2", member2)
	//				master.SAdd(ctx, "set1", member3)
	//				master.SAdd(ctx, "set2", member3)
	//				master.SAdd(ctx, "set1", member4)
	//				master.SAdd(ctx, "set2", member4)
	//				master.SAdd(ctx, "set1", member5)
	//				master.SAdd(ctx, "set2", member5)
	//				master.SAdd(ctx, "set1", member6)
	//				master.SAdd(ctx, "set2", member6)
	//				master.SAdd(ctx, "dest_set", randStr(5))
	//				master.SInterStore(ctx, "dest_set", "set1", "set2")
	//				master.SAdd(ctx, "dest_set", randStr(5))
	//			}
	//		}
	//
	//		threads := make([]*sync.WaitGroup, 10)
	//		for i := 0; i < 10; i++ {
	//			wg := &sync.WaitGroup{}
	//			wg.Add(1)
	//			threads[i] = wg
	//
	//			go func() {
	//				sinterstoreThread()
	//				wg.Done()
	//			}()
	//		}
	//
	//		for _, wg := range threads {
	//			wg.Wait()
	//		}
	//
	//		delayReplication()
	//
	//		mDestSet := master.SMembers(ctx, "dest_set").Val()
	//		sDestSet := slave.SMembers(ctx, "dest_set").Val()
	//
	//		Expect(sDestSet).To(Equal(mDestSet))
	//	})
	//
	//	It("should ZUnionStore", func() {
	//		mu.Lock()
	//		defer mu.Unlock()
	//
	//		zunionstoreThread := func() {
	//			for i := 0; i < 10; i++ {
	//				master.ZAdd(ctx, "zset1", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: randStr(5),
	//				})
	//				master.ZAdd(ctx, "zset2", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: randStr(5),
	//				})
	//				master.ZAdd(ctx, "zset2", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: randStr(5),
	//				})
	//				master.ZAdd(ctx, "zset1", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: randStr(5),
	//				})
	//				master.ZAdd(ctx, "zset2", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: randStr(5),
	//				})
	//				master.ZAdd(ctx, "zset1", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: randStr(5),
	//				})
	//				master.ZAdd(ctx, "zset2", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: randStr(5),
	//				})
	//				master.ZAdd(ctx, "zset2", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: randStr(5),
	//				})
	//				master.ZAdd(ctx, "zset1", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: randStr(5),
	//				})
	//				master.ZAdd(ctx, "zset1", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: randStr(5),
	//				})
	//				master.ZAdd(ctx, "zset2", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: randStr(5),
	//				})
	//				master.ZAdd(ctx, "zset1", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: randStr(5),
	//				})
	//				master.ZUnionStore(ctx, "zset_out", &redis.ZStore{
	//					Keys:      []string{"zset1", "zset2"},
	//					Weights:   []float64{1, 1},
	//					Aggregate: "sum",
	//				})
	//				master.ZAdd(ctx, "zset_out", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: randStr(5),
	//				})
	//			}
	//		}
	//
	//		threads := make([]*sync.WaitGroup, 10)
	//		for i := 0; i < 10; i++ {
	//			wg := &sync.WaitGroup{}
	//			wg.Add(1)
	//			threads[i] = wg
	//
	//			go func() {
	//				zunionstoreThread()
	//				wg.Done()
	//			}()
	//		}
	//
	//		for _, wg := range threads {
	//			wg.Wait()
	//		}
	//
	//		delayReplication()
	//
	//		mZSetOut := master.ZRangeWithScores(ctx, "zset_out", 0, -1).Val()
	//		sZSetOut := slave.ZRangeWithScores(ctx, "zset_out", 0, -1).Val()
	//
	//		Expect(sZSetOut).To(Equal(mZSetOut))
	//	})
	//
	//	It("should ZInterStore", func() {
	//		mu.Lock()
	//		defer mu.Unlock()
	//
	//		zinterstoreThread := func() {
	//			for i := 0; i < 10; i++ {
	//				member := randStr(5)
	//				member2 := randStr(5)
	//				member3 := randStr(5)
	//				member4 := randStr(5)
	//				master.ZAdd(ctx, "zset1", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: member,
	//				})
	//				master.ZAdd(ctx, "zset2", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: member,
	//				})
	//				master.ZAdd(ctx, "zset1", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: member2,
	//				})
	//				master.ZAdd(ctx, "zset2", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: member2,
	//				})
	//				master.ZAdd(ctx, "zset1", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: member3,
	//				})
	//				master.ZAdd(ctx, "zset2", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: member3,
	//				})
	//				master.ZAdd(ctx, "zset1", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: member4,
	//				})
	//				master.ZAdd(ctx, "zset2", redis.Z{
	//					Score:  float64(randomInt(1, 5)),
	//					Member: member4,
	//				})
	//				master.ZInterStore(ctx, "zset_out", &redis.ZStore{
	//					Keys: []string{"zset1", "zset2"},
	//				})
	//			}
	//		}
	//
	//		threads := make([]*sync.WaitGroup, 10)
	//		for i := 0; i < 10; i++ {
	//			wg := &sync.WaitGroup{}
	//			wg.Add(1)
	//			threads[i] = wg
	//
	//			go func() {
	//				zinterstoreThread()
	//				wg.Done()
	//			}()
	//		}
	//
	//		for _, wg := range threads {
	//			wg.Wait()
	//		}
	//
	//		delayReplication()
	//
	//		mZSetOut := master.ZRangeWithScores(ctx, "zset_out", 0, -1).Val()
	//		sZSetOut := slave.ZRangeWithScores(ctx, "zset_out", 0, -1).Val()
	//
	//		Expect(sZSetOut).To(Equal(mZSetOut))
	//	})
	//
	//	It("should SUnionStore replication", func() {
	//		mu.Lock()
	//		defer mu.Unlock()
	//
	//		sunionstoreThread := func() {
	//			for i := 0; i < 10; i++ {
	//				master.SAdd(ctx, "set1", randStr(5))
	//				master.SAdd(ctx, "set2", randStr(5))
	//				master.SAdd(ctx, "set1", randStr(5))
	//				master.SAdd(ctx, "set1", randStr(5))
	//				master.SAdd(ctx, "set2", randStr(5))
	//				master.SAdd(ctx, "set1", randStr(5))
	//				master.SAdd(ctx, "set1", randStr(5))
	//				master.SAdd(ctx, "set2", randStr(5))
	//				master.SAdd(ctx, "set2", randStr(5))
	//				master.SAdd(ctx, "set2", randStr(5))
	//				master.SAdd(ctx, "set1", randStr(5))
	//				master.SAdd(ctx, "set2", randStr(5))
	//				master.SAdd(ctx, "set1", randStr(5))
	//				master.SAdd(ctx, "set2", randStr(5))
	//				master.SUnionStore(ctx, "set_out", "set1", "set2")
	//			}
	//		}
	//
	//		threads := make([]*sync.WaitGroup, 10)
	//		for i := 0; i < 10; i++ {
	//			wg := &sync.WaitGroup{}
	//			wg.Add(1)
	//			threads[i] = wg
	//
	//			go func() {
	//				sunionstoreThread()
	//				wg.Done()
	//			}()
	//		}
	//
	//		for _, wg := range threads {
	//			wg.Wait()
	//		}
	//
	//		delayReplication()
	//
	//		mSetOut := master.SMembers(ctx, "set_out").Val()
	//		sSetOut := slave.SMembers(ctx, "set_out").Val()
	//
	//		Expect(sSetOut).To(Equal(mSetOut))
	//	})
	//
	//	It("should BITOP", func() {
	//		mu.Lock()
	//		defer mu.Unlock()
	//
	//		bitopThread := func() {
	//			for i := 0; i < 100; i++ {
	//				offset1 := randomInt(0, 100)
	//				offset2 := randomInt(0, 100)
	//				value1 := randomInt(0, 1)
	//				value2 := randomInt(0, 1)
	//
	//				master.SetBit(ctx, "bitkey1", int64(offset1), value1)
	//				master.SetBit(ctx, "bitkey2", int64(offset1), value1)
	//				master.BitOpAnd(ctx, "AND", "bitkey_out1", "bitkey1", "bitkey2")
	//				master.SetBit(ctx, "bitkey1", int64(offset1+offset2), value2)
	//				master.SetBit(ctx, "bitkey2", int64(offset2), value2)
	//				master.BitOpAnd(ctx, "OR", "bitkey_out2", "bitkey1", "bitkey2")
	//			}
	//		}
	//
	//		threads := make([]*sync.WaitGroup, 10)
	//		for i := 0; i < 10; i++ {
	//			wg := &sync.WaitGroup{}
	//			wg.Add(1)
	//			threads[i] = wg
	//
	//			go func() {
	//				bitopThread()
	//				wg.Done()
	//			}()
	//		}
	//
	//		for _, wg := range threads {
	//			wg.Wait()
	//		}
	//
	//		delayReplication()
	//
	//		mKeyOutCount1 := master.BitCount(ctx, "bitkey_out1", nil).Val()
	//		sKeyOutCount1 := slave.BitCount(ctx, "bitkey_out1", nil).Val()
	//
	//		mKeyOutCount2 := master.BitCount(ctx, "bitkey_out2", nil).Val()
	//		sKeyOutCount2 := slave.BitCount(ctx, "bitkey_out2", nil).Val()
	//
	//		Expect(sKeyOutCount1).To(Equal(mKeyOutCount1))
	//		Expect(sKeyOutCount2).To(Equal(mKeyOutCount2))
	//	})
	//
	//	It("should PFMerge", func() {
	//		mu.Lock()
	//		defer mu.Unlock()
	//
	//		pfmergeThread := func() {
	//			master.PFAdd(ctx, "hll1", randStr(5))
	//			master.PFAdd(ctx, "hll2", randStr(5))
	//			master.PFAdd(ctx, "hll2", randStr(5))
	//			master.PFAdd(ctx, "hll1", randStr(5))
	//			master.PFAdd(ctx, "hll2", randStr(5))
	//			master.PFAdd(ctx, "hll1", randStr(5))
	//			master.PFAdd(ctx, "hll2", randStr(5))
	//			master.PFAdd(ctx, "hll1", randStr(5))
	//			master.PFAdd(ctx, "hll_out", randStr(5))
	//			master.PFMerge(ctx, "hll_out", "hll1", "hll2")
	//			master.PFAdd(ctx, "hll_out", randStr(5))
	//		}
	//
	//		threads := make([]*sync.WaitGroup, 50)
	//		for i := 0; i < 50; i++ {
	//			wg := &sync.WaitGroup{}
	//			wg.Add(1)
	//			threads[i] = wg
	//
	//			go func() {
	//				pfmergeThread()
	//				wg.Done()
	//			}()
	//		}
	//
	//		for _, wg := range threads {
	//			wg.Wait()
	//		}
	//
	//		delayReplication()
	//
	//		mHllOut := master.PFCount(ctx, "hll_out").Val()
	//		sHllOut := slave.PFCount(ctx, "hll_out").Val()
	//
	//		Expect(sHllOut).To(Equal(mHllOut))
	//	})
	//
	//	It("should MigrateSlot", func() {
	//		mu.Lock()
	//		defer mu.Unlock()
	//
	//		master.ConfigSet(ctx, "slotmigrate", "yes")
	//		slave.ConfigSet(ctx, "slotmigrate", "no")
	//
	//		setKey1 := "setKey_000"
	//		setKey2 := "setKey_001"
	//		setKey3 := "setKey_002"
	//		setKey4 := "setKey_store"
	//
	//		delayReplication()
	//
	//		for i := 0; i < 5; i++ {
	//			master.SAdd(ctx, setKey1, randStr(5))
	//			master.SAdd(ctx, setKey2, randStr(5))
	//			master.SAdd(ctx, setKey1, randStr(5))
	//			master.SAdd(ctx, setKey2, randStr(5))
	//			master.SAdd(ctx, setKey1, randStr(5))
	//			master.SAdd(ctx, setKey2, randStr(5))
	//			master.SAdd(ctx, setKey1, randStr(5))
	//			master.SAdd(ctx, setKey2, randStr(5))
	//			master.SAdd(ctx, setKey3, randStr(5))
	//			master.SAdd(ctx, setKey1, randStr(5))
	//			master.SAdd(ctx, setKey3, randStr(5))
	//			master.SAdd(ctx, setKey2, randStr(5))
	//			master.SAdd(ctx, setKey3, randStr(5))
	//			master.SAdd(ctx, setKey2, randStr(5))
	//			master.SAdd(ctx, setKey1, randStr(5))
	//			master.SAdd(ctx, setKey3, randStr(5))
	//			master.SDiffStore(ctx, setKey4, setKey1, setKey2)
	//		}
	//
	//		Expect(slave.SlaveOf(ctx, masterIP, masterPort).Err()).NotTo(HaveOccurred())
	//		time.Sleep(20 * time.Second)
	//
	//		mSet1 := master.SMembers(ctx, setKey1).Val()
	//		mSet2 := master.SMembers(ctx, setKey2).Val()
	//		mSet3 := master.SMembers(ctx, setKey3).Val()
	//		mDestSet := master.SMembers(ctx, setKey4).Val()
	//
	//		sSet1 := slave.SMembers(ctx, setKey1).Val()
	//		sSet2 := slave.SMembers(ctx, setKey2).Val()
	//		sSet3 := slave.SMembers(ctx, setKey3).Val()
	//		sDestSet := slave.SMembers(ctx, setKey4).Val()
	//
	//		Expect(sSet1).To(Equal(mSet1))
	//		Expect(sSet2).To(Equal(mSet2))
	//		Expect(sSet3).To(Equal(mSet3))
	//		Expect(sDestSet).To(Equal(mDestSet))
	//
	//		// Disconnect master and slave
	//		Expect(slave.SlaveOf(ctx, "no", "one").Err()).NotTo(HaveOccurred())
	//
	//		// Add more data to master
	//		for i := 0; i < 5; i++ {
	//			master.SAdd(ctx, setKey1, randStr(5))
	//			master.SAdd(ctx, setKey2, randStr(5))
	//			master.SAdd(ctx, setKey1, randStr(5))
	//			master.SAdd(ctx, setKey2, randStr(5))
	//			master.SAdd(ctx, setKey1, randStr(5))
	//			master.SAdd(ctx, setKey2, randStr(5))
	//			master.SAdd(ctx, setKey1, randStr(5))
	//			master.SAdd(ctx, setKey2, randStr(5))
	//			master.SAdd(ctx, setKey3, randStr(5))
	//			master.SAdd(ctx, setKey1, randStr(5))
	//			master.SAdd(ctx, setKey3, randStr(5))
	//			master.SAdd(ctx, setKey2, randStr(5))
	//			master.SAdd(ctx, setKey3, randStr(5))
	//			master.SAdd(ctx, setKey2, randStr(5))
	//			master.SAdd(ctx, setKey1, randStr(5))
	//			master.SAdd(ctx, setKey3, randStr(5))
	//			master.SDiffStore(ctx, setKey4, setKey1, setKey2)
	//		}
	//
	//		// Reconnect master and slave
	//		Expect(slave.SlaveOf(ctx, masterIP, masterPort).Err()).NotTo(HaveOccurred())
	//		time.Sleep(25 * time.Second)
	//
	//		mSet1 = master.SMembers(ctx, setKey1).Val()
	//		mSet2 = master.SMembers(ctx, setKey2).Val()
	//		mSet3 = master.SMembers(ctx, setKey3).Val()
	//		mDestSet = master.SMembers(ctx, setKey4).Val()
	//
	//		time.Sleep(15 * time.Second)
	//
	//		sSet1 = slave.SMembers(ctx, setKey1).Val()
	//		sSet2 = slave.SMembers(ctx, setKey2).Val()
	//		sSet3 = slave.SMembers(ctx, setKey3).Val()
	//		sDestSet = slave.SMembers(ctx, setKey4).Val()
	//
	//		Expect(sSet1).To(Equal(mSet1))
	//		Expect(sSet2).To(Equal(mSet2))
	//		Expect(sSet3).To(Equal(mSet3))
	//		Expect(sDestSet).To(Equal(mDestSet))
	//
	//		// Slave node should not have slot key
	//		sKeys, _ := slave.Keys(ctx, "*").Result()
	//		for _, key := range sKeys {
	//			Expect(key).NotTo(Or(
	//				ContainSubstring("_internal:slotkey:4migrate:"),
	//				ContainSubstring("_internal:slottag:4migrate:"),
	//			))
	//		}
	//	})
	//})
})
