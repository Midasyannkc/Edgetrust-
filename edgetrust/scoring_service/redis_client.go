// A minimal Redis RESP (REdis Serialization Protocol) client implementing
// exactly the one command this service needs: HGETALL. Written by hand,
// using only net and bufio from the Go standard library, because pulling
// in go-redis would require reaching proxy.golang.org, which this build
// environment cannot do. Production deployment would swap this for
// go-redis/redis/v9 without changing any caller code, since getFeatures()
// in main.go is the only thing that touches this file.
package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
)

func dialRedis(addr string) (net.Conn, error) {
	return net.Dial("tcp", addr)
}

// respHGetAll sends "HGETALL key" as a RESP array and parses the resulting
// flat array reply into a map.
func respHGetAll(conn net.Conn, key string) (map[string]string, error) {
	cmd := fmt.Sprintf("*2\r\n$7\r\nHGETALL\r\n$%d\r\n%s\r\n", len(key), key)
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return nil, err
	}

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")

	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("unexpected RESP reply: %s", line)
	}
	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}

	values := make([]string, 0, count)
	for i := 0; i < count; i++ {
		bulkHeader, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		bulkHeader = strings.TrimRight(bulkHeader, "\r\n")
		if len(bulkHeader) == 0 || bulkHeader[0] != '$' {
			return nil, fmt.Errorf("unexpected bulk header: %s", bulkHeader)
		}
		n, err := strconv.Atoi(bulkHeader[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, n+2) // +2 for trailing \r\n
		if _, err := reader.Read(buf); err != nil {
			return nil, err
		}
		values = append(values, string(buf[:n]))
	}

	result := make(map[string]string)
	for i := 0; i+1 < len(values); i += 2 {
		result[values[i]] = values[i+1]
	}
	return result, nil
}
