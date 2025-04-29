package ltqd

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net"
)

func UniqRands(quantity int, maxval int) []int {
	if maxval < quantity {
		quantity = maxval
	}

	intSlice := make([]int, maxval)
	for i := 0; i < maxval; i++ {
		intSlice[i] = i
	}

	for i := 0; i < quantity; i++ {
		randInt, err := rand.Int(rand.Reader, big.NewInt(int64(maxval)))
		if err != nil {
			panic(fmt.Sprintf("failed to generate random number: %v", err))
		}
		j := int(randInt.Int64()) + i
		// swap
		intSlice[i], intSlice[j] = intSlice[j], intSlice[i]
		maxval--

	}
	return intSlice[0:quantity]
}

func TypeOfAddr(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err == nil {
		return "tcp"
	}
	return "unix"
}

func getBackendName(topicName, channelName string) string {
	backendName := topicName + ":" + channelName
	return backendName
}
