package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"github.com/zekroTJA/timedmap"
	"time"
	"strconv"
)

func main() {
	fmt.Println("Logs from your program will appear here!")
	args := os.Args;
	port := "6379"
	role := "master"
	master_host := "0.0.0.0"
	master_port := "6379"
	// fmt.Println(args)
	if(len(args) > 2 && args[1] == "--port"){
		port = args[2]
	}

	if(len(args) > 4 && args[3] == "--replicaof"){
		role = "slave"
		master_details := strings.Split(args[4], " ");
		master_host = master_details[0]
		master_port = master_details[1]

	}
	
	listner, err := net.Listen("tcp", "0.0.0.0:" + port)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer listner.Close()
	for {
		fmt.Println("a command requested")
		con, err := listner.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleClient(con, role, master_host, master_port)
	}

}

func handleClient(conn net.Conn, role string, master_host string, master_port string) {
	// Ensure we close the connection after we're done
	defer conn.Close()
	store := timedmap.New(50 * time.Millisecond)
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			fmt.Println("Error", err)
			return
		}
		command_list := strings.Split(string(buf[:n]), "\r\n")
		fmt.Println(command_list)
		if(command_list[2] == "PING"){
			conn.Write([]byte("+PONG\r\n"))
		}else if(command_list[2] == "ECHO"){
			echo_message := strings.Join(command_list[3:],"\r\n")
			conn.Write([]byte(echo_message))
		}else if(command_list[2] == "SET"){
			if(len(command_list) >= 10){
				exp_time, _ := strconv.Atoi(command_list[10])
				store.Set(command_list[3] + command_list[4],command_list[5] + "\r\n" + command_list[6] + "\r\n",time.Millisecond * time.Duration(exp_time))
				
			}else{
				store.Set(command_list[3] + command_list[4],command_list[5] + "\r\n" + command_list[6] + "\r\n",time.Hour * 24)
			}
			conn.Write([]byte("+OK\r\n"))
		}else if(command_list[2] == "GET"){
			conn.Write([]byte(printKeyVal(store, command_list[3] + command_list[4])))
		}else if(command_list[2] == "INFO") {
			if(role == "master"){
				conn.Write([]byte("$11\r\nrole:master\r\n"))
			}else{
				conn.Write([]byte("$10\r\nrole:slave\r\n"))
			}
		}
	}	
}

func printKeyVal(tm *timedmap.TimedMap, key string) string {
	d, ok := tm.GetValue(key).(string)
	if !ok {
		return "$-1\r\n"
	}

	return d;
}
