package main

import (
	"fmt"
	"strings"
	"net"
	"os"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	con , err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	buffer := make([]byte, 2048)
	byte_count , _ := con.Read(buffer)

	command_list := strings.Split(string(buffer[:byte_count]) , "\r\n")
	// command_list = strings.Split(command_list[0], " ")
	fmt.Println(command_list[2])
	if(command_list[2] == "PING"){
		if(len(command_list[3]) == 0){
			con.Write([]byte("+PONG\r\n"))
		}else{
			con.Write([]byte(strings.Join(command_list[3:], "\r\n")))
		}
	}

}
