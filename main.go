package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
)

func main() {
	listener, listenerErr := net.Listen("tcp", ":6379")
	if listenerErr != nil {
		panic(listenerErr)
	}
	fmt.Println("Listening on :6379")

	for {
		clientConnection, connErr := listener.Accept()
		if connErr != nil {
			panic(connErr)
		}

		fmt.Println("Connected to client", clientConnection.RemoteAddr())

		defer clientConnection.Close()

		for {
			buffer := make([]byte, 1024)
			_, readErr := clientConnection.Read(buffer)
			if readErr != nil {
				if readErr == io.EOF {
					fmt.Println("Client disconnected", clientConnection.RemoteAddr())
					break
				}
				panic(readErr)
			}

			// reading input
			reader := bufio.NewReader(bytes.NewBuffer(buffer))
			b, err := reader.ReadByte()
			if err != nil {
				panic(err)
			}
			if b != byte('*') {
				fmt.Println(fmt.Sprintf("Invalid byte received - %b. Command skipped: %v", b, clientConnection.RemoteAddr()))
				continue
			}

			commandCount, err := getInt(reader)
			if err != nil {
				fmt.Println(fmt.Sprintf("Invalid byte received - %b. Command skipped: %v", b, clientConnection.RemoteAddr()))
				continue
			}
			reader.ReadByte()
			reader.ReadByte()

			commands, err := getCommands(reader, commandCount)
			if err != nil {
				fmt.Println(fmt.Sprintf("Error in processing command - %v. Command skipped: %v", err, clientConnection.RemoteAddr()))
				continue
			}

			fmt.Println(commands)

			clientConnection.Write([]byte("+OK\r\n"))
		}
	}
}

func getInt(reader *bufio.Reader) (int64, error) {
	size, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(string(size), 10, 64)
}

func getCommands(reader *bufio.Reader, commandCount int64) ([]string, error) {
	var commands []string

	for i := 0; i < int(commandCount); i++ {
		reader.ReadByte() // '$'
		length, err := getInt(reader)
		if err != nil {
			return nil, err
		}

		reader.ReadByte()
		reader.ReadByte()

		str := make([]byte, length)
		_, err = reader.Read(str)
		if err != nil {
			return nil, err
		}
		commands = append(commands, string(str))
		reader.ReadByte()
		reader.ReadByte()
	}

	return commands, nil
}
