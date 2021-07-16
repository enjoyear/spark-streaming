package main

import (
	"fmt"
	"net"
	"time"
)

func main() {
	//Connect locally using "telnet localhost 2001"
	for port := 2000; port < 2005; port++ {
		fmt.Printf("Port is %v\n", port)
		listenAddress := fmt.Sprintf("localhost:%d", port)
		l, err := net.Listen("tcp", listenAddress)
		if err != nil {
			fmt.Printf("Error in listening on %v: %v\n", listenAddress, err)
			return
		}

		go func() {
			for {
				fmt.Printf("Waiting on %s\n", listenAddress)
				conn, err := l.Accept()
				if err != nil {
					fmt.Printf("Error in accepting on %v: %v\n", listenAddress, err)
					l.Close()
					return
				}
				tcpConn := conn.(*net.TCPConn)
				fmt.Printf("Build new connection for %s\n", tcpConn)
				fmt.Printf("Local Address: %s\n", tcpConn.LocalAddr())
				fmt.Printf("Remote Address: %s\n", tcpConn.RemoteAddr())
				tcpConn.SetKeepAlive(true)
				//tcpConn.SetKeepAlive(true)
			}
		}()
	}

	time.Sleep(1000 * time.Second)
}
