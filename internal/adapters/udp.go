package adapters

import (
	"encoding/json"
	"github.com/isatay012or02/kafka-diode-caster/internal/domain"
	"net"
)

type UDPSender struct {
	conn *net.UDPConn
}

func NewUDPSender(address string) (*UDPSender, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, err
	}
	return &UDPSender{conn: conn}, nil
}

func (s *UDPSender) Send(message domain.Message) error {
	jsonMsg, err := json.Marshal(message)
	if err != nil {
		return err
	}

	_, err = s.conn.Write(jsonMsg)
	return err
}
