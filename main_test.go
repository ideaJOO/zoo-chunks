package main

import (
	"testing"
	"time"
)

func TestGetRandomChunkBlobSizes(t *testing.T) {

	var fSzie uint32 = 1024

	for i := 0; i < 40; i++ {
		temp := GetRandomChunkBlobSizes(fSzie)
		var sum uint32 = 0
		for _, size := range temp {
			//t.Log(size)
			sum = sum + uint32(size)
		}
		if fSzie == sum {
			println("SUCCESS")
		} else {
			println("FAIL", len(temp), fSzie, sum)
		}
		time.Sleep(time.Second)
	}
}

func TestSplitChunkBlobs2(t *testing.T) {
	SplitChunkBlobsAzure()


	time.Sleep(time.Second * 20)
	println("FINISH")
}

func TestCombineChunkBlobsAzure(t *testing.T) {
	CombineChunkBlobsAzure()
}
