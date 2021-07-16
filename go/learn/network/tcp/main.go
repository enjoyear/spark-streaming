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
		listenAddress := fmt.Sprintf("localhost:%d", port)
		l, err := net.Listen("tcp", listenAddress)
		if err != nil {
			fmt.Printf("Error in listening on %v: %v\n", listenAddress, err)
			return
		}

		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("Error in accepting on %v: %v\n", listenAddress, err)
			l.Close()
		}
		tcpConn := conn.(*net.TCPConn)
		fmt.Printf("%s\n", tcpConn)
		fmt.Printf("Local Address: %s\n", tcpConn.LocalAddr())
		fmt.Printf("Remote Address: %s\n", tcpConn.RemoteAddr())
		tcpConn.SetKeepAlive(true)
		//tcpConn.SetKeepAlive(true)
	}
}
