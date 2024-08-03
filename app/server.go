package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
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
		_, err := conn.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			fmt.Println("Error", err)
			return
		}
		conn.Write([]byte("+PONG\r\n"))
	}
}
