package main

import (
	"fmt"
	"net"
)

func main() {
	//Connect locally using "telnet localhost 2001"
	port := 2000
	for {
		port++
		fmt.Printf("Port is %v\n", port)
		l, _ := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
		conn, _ := l.Accept()
		tcpConn := conn.(*net.TCPConn)
		fmt.Printf("%s\n", tcpConn)
		fmt.Printf("Local Address: %s\n", tcpConn.LocalAddr())
		fmt.Printf("Remote Address: %s\n", tcpConn.RemoteAddr())
		tcpConn.SetKeepAlive(true)
		//tcpConn.SetKeepAlive(true)
	}
}
