package main

import (
	"fmt"
	"os"
	"math"
	"strconv"
	"io/ioutil"
	"bufio"
	"math/rand"
	"time"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"net/url"
	"context"
	"bytes"
	"sync"
)

type azBlobInfo struct {
	accountName   string
	containerName string
	accountKey    string
	blobName      string
}

const (
	azContainerName = ""
	azAccountName   = ""
	azAccountKey    = ""
)

const fileName = "./video.mp4"

func main() {
	fmt.Println("PROTOTYPE")
	//SplitChunkBlobs()
	//CombineChunkBlobs()
	SplitChunkBlobsAzure()
}

func (blobInfo azBlobInfo) GetAzBlobURL() (blobURL azblob.BlockBlobURL) {

	azAccountName := blobInfo.accountName
	azContainerName := blobInfo.containerName
	azAccountKey := blobInfo.accountKey
	azBlobName := blobInfo.blobName

	azUrlStr := fmt.Sprintf("https://%s.blob.core.windows.net/%s", azAccountName, azContainerName)
	azURL, _ := url.Parse(azUrlStr)
	pipeline := azblob.NewPipeline(
		azblob.NewSharedKeyCredential(azAccountName, azAccountKey),
		azblob.PipelineOptions{})
	containerURL := azblob.NewContainerURL(*azURL, pipeline)

	blobURL = containerURL.NewBlockBlobURL(azBlobName)

	return blobURL
}

func SplitChunkBlobs() {

	const chunkBlobSize = 1 * (1 << 20) // 1MB

	//var minChunkBlobSize uint32 = 1 * (1 << 10)
	//var limitChunkBlobSize uint32 = 4 * (1 << 20)

	// 4. Set Split N-ChunkBlobs
	// 5. [Optional] Set M-TrickeryChunkBlobs
	// 6. Cal SHA256-ChunkBlobs => ChunkBlobIDs
	// 7. POST Async [ N + M ] ChunkFiles + ChunkBlobIDs
	// 8. END

	// 1. Open Binary File
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
	}
	defer func() {
		if file != nil {
			file.Close()
		}
	}()

	// 2. Get FileInfo.
	fileInfo, _ := file.Stat()
	fName := fileInfo.Name()
	fMode := fileInfo.Mode().String()
	fSize := fileInfo.Size()
	fmt.Printf("FILE INFO : Name=[%s], Mode=[%s], Size=[%d]", fName, fMode, fSize)

	// 3. Get Set of ChunksSize
	numChunkBlobs := uint64(math.Ceil(float64(fSize) / float64(chunkBlobSize)))
	//maxChunkBlobSize := uint32(math.Min(float64(limitChunkBlobSize), float64(fSize)))
	//GetRandomChunkBlobSizes(fSize)

	fmt.Printf("Number of Chunks Blobs = %d", numChunkBlobs)
	// 이 부분에서 생성된 Blob CHunks 를 Range 로 돌려야 한다.

	for i := uint64(0); i < numChunkBlobs; i++ {
		partSize := int(math.Min(chunkBlobSize, float64(fSize-int64(i*chunkBlobSize))))
		partBuffer := make([]byte, partSize)

		file.Read(partBuffer)

		chunkBlobName := "CHUNK_BLOB_" + strconv.FormatUint(i, 10)
		if _, err = os.Create(chunkBlobName); err != nil {
			fmt.Printf("ERR-Create-ChunkBlob: %s", err)
			os.Exit(1)
		}
		ioutil.WriteFile(chunkBlobName, partBuffer, os.ModeAppend)
		fmt.Printf("SUCCESS: %s", chunkBlobName)
	}
}

func GetRandomChunkBlobSizes(targetSize uint32) (chunkSizes []uint32) {
	type MinRange struct {
		r0, r1 uint32
	}
	type MaxRange struct {
		r0, r1 uint32
	}
	minR := MinRange{r0: 32, r1: 1024}
	maxR := MaxRange{r0: 2 * 1024, r1: 4 * 1024 * 1024}
	remainderSize := targetSize

	// Cal Min
	var minSize uint32
	if minSize = targetSize / 100; minSize > minR.r1 {
		minSize = minR.r1
	} else if minSize < minR.r0 {
		minSize = minR.r0
	}

	// Cal Max
	var maxSize uint32
	if maxSize = targetSize / 10; maxSize > maxR.r1 {
		maxSize = maxR.r1
	} else if maxSize < maxR.r0 {
		maxSize = maxR.r0
	}

L1:
	for {
		rand.NewSource(time.Now().Unix())
		randomChunkSize := uint32(rand.Int31n(int32(maxSize-minSize))) + minSize
		//println("randomChunkSize ", randomChunkSize)

		if remainderSize <= minSize {
			chunkSizes = append(chunkSizes, uint32(remainderSize))
			break L1
		} else if remainderSize <= randomChunkSize {
			chunkSizes = append(chunkSizes, uint32(remainderSize))
			break L1
		} else {
			chunkSizes = append(chunkSizes, randomChunkSize)
			remainderSize = remainderSize - randomChunkSize
			// loop
		}
	}

	sum := uint32(0)
	for _, tmpSize := range chunkSizes {
		sum = sum + tmpSize
	}

	if targetSize != sum {

		fmt.Printf("ERROR SPLIT CHUNK SIZEs \n")

		return nil
	} else {

		fmt.Printf("SUCCESS SPLIT CHUNK SIZEs [%d] \n", sum)

		return chunkSizes
	}

}

func CombineChunkBlobs() {
	// 1. Get ChunksBlobIDs
	// 2. GET Async N-ChunkBlobs by ChunksBlobIDs
	// 3. Assembly File with N-ChunkBlobs
	// 4.

	numChunkBlobs := 11
	chunkBlobNamePrefix := "CHUNK_BLOB_"
	newFileName := "temp"

	_, err := os.Create(newFileName)
	if err != nil {
		fmt.Printf("ERR CreateFile: %s", err)
		os.Exit(1)
	}

	file, err := os.OpenFile(newFileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		fmt.Printf("ERR CreateFile: %s", err)
		os.Exit(1)
	}

	var filePosition int64 = 0

	for j := uint64(0); j < uint64(numChunkBlobs); j++ {
		currentChunkBlobName := chunkBlobNamePrefix + strconv.FormatUint(j, 10)
		currChunkBlob, err := os.Open(currentChunkBlobName)
		if err != nil {
			fmt.Printf("%s", err)
			os.Exit(1)
		}
		defer currChunkBlob.Close()

		chunkBlobInfo, err := currChunkBlob.Stat()

		if err != nil {
			fmt.Printf("%s", err)
			os.Exit(1)
		}
		var sizeCurrChunkBlob int64 = chunkBlobInfo.Size()
		chunkBufferBytes := make([]byte, sizeCurrChunkBlob)

		fmt.Printf("Curr Position: [ %d ]", filePosition)

		filePosition = filePosition + sizeCurrChunkBlob
		reader := bufio.NewReader(currChunkBlob)
		if _, err = reader.Read(chunkBufferBytes); err != nil {
			fmt.Printf("ERR Append NewFile : %s", err)
			os.Exit(1)
		}
		n, err := file.Write(chunkBufferBytes)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		file.Sync()
		chunkBufferBytes = nil
		fmt.Println("Written ", n, " bytes")
		fmt.Println("Recombining part [%d]", j)
	}

	// https://socketloop.com/tutorials/golang-recombine-chunked-files-example
	file.Close()
}

func CombineChunkBlobsAzure() {

	numChunkBlobs := 17
	chunkBlobNamePrefix := "video.mp4"
	newFileName := "aztemp.mp4"

	_, err := os.Create(newFileName)
	if err != nil {
		fmt.Printf("ERR CreateFile: %s", err)
		os.Exit(1)
	}

	file, err := os.OpenFile(newFileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		fmt.Printf("ERR CreateFile: %s", err)
		os.Exit(1)
	}

	for i := 0; i < numChunkBlobs; i++ {

		tBlobName := fmt.Sprintf("%s_%d_%d", chunkBlobNamePrefix, i, numChunkBlobs)
		//=========================
		// Azure DownLoad Blob
		//=========================
		blobInfo := azBlobInfo{
			containerName: azContainerName,
			accountName:   azAccountName,
			accountKey:    azAccountKey,
			blobName:      tBlobName}
		stream := azblob.NewDownloadStream(context.Background(), blobInfo.GetAzBlobURL().GetBlob, azblob.DownloadStreamOptions{})

		blobData := &bytes.Buffer{}
		if _, err = blobData.ReadFrom(stream); err != nil {

			return
		}

		n, err := file.Write(blobData.Bytes())

		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("SUCCESS [%d] [%s]", n, tBlobName)
	}

	// https://socketloop.com/tutorials/golang-recombine-chunked-files-example
	file.Close()
}

func SplitChunkBlobsAzure() {

	// 1. Open Binary File
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
	}
	defer func() {
		if file != nil {
			file.Close()
		}
	}()

	// 2. Get FileInfo.
	fileInfo, _ := file.Stat()
	fName := fileInfo.Name()
	fMode := fileInfo.Mode().String()
	fSize := fileInfo.Size()
	fmt.Printf("FILE INFO : Name=[%s], Mode=[%s], Size=[%d] \n", fName, fMode, fSize)

	chunkBlobSizeSet := GetRandomChunkBlobSizes(uint32(fSize))

	fmt.Printf("== File Size : %d \n", fSize)
	fmt.Printf("== TOTAL ChunkBlobs Count : %d \n", len(chunkBlobSizeSet))

	// Async Golang 부분을 해야 한다.
	// Async 로 하면.
	var wg sync.WaitGroup

	for idx, chunkBlobSize := range chunkBlobSizeSet {

		partSize := int(chunkBlobSize)
		partBuffer := make([]byte, partSize)
		file.Read(partBuffer)

		tBlobName := fmt.Sprintf("%s_%d_%d", fileName, idx, len(chunkBlobSizeSet))
		tBlobSize := fmt.Sprintf("%d", chunkBlobSize)

		blobInfo := azBlobInfo{
			containerName: azContainerName,
			accountName:   azAccountName,
			accountKey:    azAccountKey,
			blobName:      tBlobName}

		fmt.Printf("BlobName: %s \n", tBlobName)

		wg.Add(1)
		go func() {
			if _, err = azblob.UploadBufferToBlockBlob(
				context.Background(),
				partBuffer,
				blobInfo.GetAzBlobURL(),
				azblob.UploadToBlockBlobOptions{BlockSize: 4 * 1024 * 1024, Parallelism: 16});
				err != nil {
				fmt.Printf("ERROR %s \n", tBlobName)
				wg.Done()
			} else {
				fmt.Printf("SUCCESS %s [%s] \n", tBlobName, tBlobSize)
				wg.Done()
			}
		}()
	}
}
