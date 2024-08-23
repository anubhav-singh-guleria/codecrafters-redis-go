package main
import (
	"bufio"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)
var info = map[string]map[string]any{
	// "server": {
	// 	"version":       "0.0.1",
	// 	"redis_version": "7.6.2",
	// },
	"replication": {
		"role": "master",
	},
}
var items = map[string]string{}
var expirations = map[string]time.Time{}
var lock = sync.RWMutex{}
var con net.Conn
var master net.Conn
var slaves = []net.Conn{}
func main() {
	replicaOf := flag.String("replicaof", "", "replicate to another redis server")
	p := flag.Int("port", 6379, "port to listen on")
	flag.Parse()
	if *replicaOf != "" { // TODO: handle --port 8080 --replicaof localhost 8181
		fmt.Println("replicating to", *replicaOf)
		fmt.Println("args", flag.Args())
		info["replication"]["role"] = "slave"
		con, err := net.Dial("tcp", *replicaOf+":"+flag.Args()[0])
		if err != nil {
			panic(err)
		}
		defer con.Close()
		con.Write([]byte("*1\r\n$4\r\nPING\r\n"))
		buf := make([]byte, 1024)
		n, _ := con.Read(buf)
		fmt.Println("ping", string(buf[:n]))
		con.Write([]byte(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n%d\r\n", *p)))
		buf = make([]byte, 1024)
		n, _ = con.Read(buf)
		fmt.Println("replconf", string(buf[:n]))
		// con.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
		// REPLCONF capa eof capa psync2
		con.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n"))
		buf = make([]byte, 1024)
		n, _ = con.Read(buf)
		fmt.Println("replconf", string(buf[:n]))
		// con.Write([]byte("*1\r\n$4\r\nINFO\r\n"))
		// buf = make([]byte, 10240)
		// n, _ = con.Read(buf)
		// replid := ""
		// for _, line := range strings.Split(string(buf[:n]), "\r\n") {
		// 	if strings.HasPrefix(line, "master_replid:") {
		// 		replid = strings.Split(line, ":")[1]
		// 		break
		// 	}
		// }
		// fmt.Println("replid", replid)
		// replid := "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
		// con.Write([]byte(fmt.Sprintf("*3\r\n$5\r\nPSYNC\r\n$%d\r\n%s\r\n$2\r\n-1\r\n", len(replid), replid)))
		con.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
		buf = make([]byte, 1024)
		n, _ = con.Read(buf)
		fmt.Println("psync", string(buf[:n]))
	}
	if info["replication"]["role"] == "master" {
		info["replication"]["master_replid"] = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
		info["replication"]["master_repl_offset"] = "0"
	}
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *p))
	if err != nil {
		panic(err)
	}
	defer l.Close()
	go func() {
		for {
			for k, v := range expirations {
				if time.Now().After(v) {
					lock.Lock()
					delete(items, k)
					delete(expirations, k)
					lock.Unlock()
				}
			}
			time.Sleep(time.Second * 5)
		}
	}()
	for {
		con, err := l.Accept()
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		go handleClient(con)
	}
}
func readCommand(c net.Conn) (string, []string, error) {
	r := bufio.NewReader(c)
	d, err := r.ReadByte()
	if err != nil && err != io.EOF {
		return "", nil, err
	}
	if d != '*' {
		return "", nil, fmt.Errorf("invalid protocol")
	}
	var expectedNumberOfArgs int
	_, err = fmt.Fscanf(r, "%d\r\n", &expectedNumberOfArgs)
	if err != nil && err != io.EOF {
		return "", nil, err
	}
	cmd := ""
	args := make([]string, expectedNumberOfArgs)
	for i := 0; i < expectedNumberOfArgs; i++ {
		d, err := r.ReadByte()
		if err != nil && err != io.EOF {
			return "", nil, err
		}
		if d != '$' {
			return "", nil, fmt.Errorf("not supported yet")
		}
		var size int
		_, err = fmt.Fscanf(r, "%d\r\n", &size)
		if err != nil && err != io.EOF {
			return "", nil, err
		}
		buf := make([]byte, size)
		_, err = io.ReadFull(r, buf)
		if err != nil && err != io.EOF {
			return "", nil, err
		}
		r.ReadByte()
		r.ReadByte()
		if i == 0 {
			cmd = strings.ToUpper(string(buf))
		} else {
			args[i-1] = string(buf)
		}
	}
	return cmd, args, nil
}
func handleClient(c net.Conn) {
	defer c.Close()
	for {
		cmd, args, err := readCommand(c)
		if err != nil {
			fmt.Println(err.Error())
			writeError(c, "ERR "+err.Error())
		}
		fmt.Printf("%s %v\n", cmd, args)
		if cmd == "" {
			return
		}
		switch cmd {
		case "PING":
			writeSimpleString(c, "PONG")
		case "ECHO":
			writeBulkString(c, args[0])
		case "SET":
			lock.Lock()
			items[args[0]] = args[1]
			lock.Unlock()
			writeSimpleString(c, "OK")
			if len(args) == 5 && strings.ToUpper(args[2]) == "PX" {
				px, err := strconv.Atoi(args[3])
				if err == nil {
					expirations[args[0]] = time.Now().Add(time.Millisecond * time.Duration(px))
				}
			}
			for _, slave := range slaves {
				slave.Write([]byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(args[0]), args[0], len(args[1]), args[1])))
			}
		case "GET":
			lock.RLock()
			val, ok := items[args[0]]
			lock.RUnlock()
			if !ok {
				writeBulkString(c, "")
			} else {
				exp, ok := expirations[args[0]]
				if ok && time.Now().After(exp) {
					writeBulkString(c, "")
					lock.Lock()
					delete(items, args[0])
					delete(expirations, args[0])
					lock.Unlock()
				} else {
					writeBulkString(c, val)
				}
			}
		case "INFO":
			if len(args) == 0 || strings.ToLower(args[0]) != "replication" {
				writeError(c, "ERR not supported yet")
				break
			}
			var sb strings.Builder
			for sk, sv := range info {
				if args[0] != "" && sk != strings.ToLower(args[0]) {
					continue
				}
				sb.WriteString("# " + sk + "\r\n")
				for k, v := range sv {
					sb.WriteString(fmt.Sprintf("%s:%s\r\n", k, v))
				}
			}
			writeBulkString(c, sb.String())
		case "REPLCONF":
			writeSimpleString(c, "OK")
			// if len(args) != 2 {
			// 	writeError(c, "ERR wrong number of arguments for 'replconf' command")
			// 	break
			// }
			// if strings.ToLower(args[0]) == "listening-port" {
			// 	info["replication"]["listening-port"] = args[1]
			// 	writeSimpleString(c, "OK")
			// } else {
			// 	writeError(c, "ERR not supported yet")
			// }
		case "PSYNC":
			slaves = append(slaves, c)
			// FULLRESYNC <REPL_ID> 0\r\n
			writeSimpleString(c, fmt.Sprintln("FULLRESYNC", info["replication"]["master_replid"], 0))
			// Send RDB file
			// https://github.com/codecrafters-io/redis-tester/blob/main/internal/assets/empty_rdb_hex.md
			b64 := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
			// decode b64 base64 encoded string into bytes variable
			b, _ := base64.StdEncoding.DecodeString(b64)
			// $<length_of_file>\r\n<contents_of_file>
			c.Write([]byte(fmt.Sprintf("$%d\r\n%s", len(b), b)))
		case "QUIT":
			return
		case "EXIT":
			return
		default:
			writeError(c, "ERR not supported yet")
		}
	}
}
// https://redis.io/docs/latest/develop/reference/protocol-spec/
func writeSimpleString(c net.Conn, msg string) {
	c.Write([]byte("+" + msg + "\r\n"))
}
func writeBulkString(c net.Conn, msg string) {
	if msg == "" {
		c.Write([]byte("$-1\r\n"))
		return
	}
	c.Write([]byte("$" + fmt.Sprint(len(msg)) + "\r\n" + msg + "\r\n"))
}
func writeError(c net.Conn, msg string) {
	c.Write([]byte("-" + msg + "\r\n"))
}