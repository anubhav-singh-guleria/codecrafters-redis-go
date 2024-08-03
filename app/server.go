package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	listner, err := net.Listen("tcp", "0.0.0.0:6379")
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
		go handleClient(con)
	}

}

func handleClient(conn net.Conn) {
	// Ensure we close the connection after we're done
	defer conn.Close()

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
			fmt.Println("echo message: "+echo_message)
			conn.Write([]byte(echo_message))
		}
	}
}
