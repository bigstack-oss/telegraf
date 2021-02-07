package sflow

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

const sampleConfig = `
  ## Address to listen for sFlow packets.
  ##   example: service_address = "udp://:6343"
  ##            service_address = "udp4://:6343"
  ##            service_address = "udp6://:6343"
  service_address = "udp://:6343"

  ## Set the size of the operating system's receive buffer.
  ##   example: read_buffer_size = "64KiB"
  # read_buffer_size = ""
`

const (
	maxPacketSize = 64 * 1024
)

type SFlow struct {
	ServiceAddress string        `toml:"service_address"`
	ReadBufferSize internal.Size `toml:"read_buffer_size"`

	Log telegraf.Logger `toml:"-"`

	addr    net.Addr
	decoder *PacketDecoder
	closer  io.Closer
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// Description answers a description of this input plugin
func (s *SFlow) Description() string {
	return "SFlow V5 Protocol Listener"
}

// SampleConfig answers a sample configuration
func (s *SFlow) SampleConfig() string {
	return sampleConfig
}

func (s *SFlow) Init() error {
	s.decoder = NewDecoder()
	s.decoder.Log = s.Log
	return nil
}

// Start starts this sFlow listener listening on the configured network for sFlow packets
func (s *SFlow) Start(acc telegraf.Accumulator) error {
	s.decoder.OnPacket(func(p *V5Format) {
		metrics, err := makeMetrics(p)
		if err != nil {
			s.Log.Errorf("Failed to make metric from packet: %s", err)
			return
		}
		for _, m := range metrics {
			acc.AddMetric(m)
		}
	})

	u, err := url.Parse(s.ServiceAddress)
	if err != nil {
		return err
	}

	conn, err := listenUDP(u.Scheme, u.Host)
	if err != nil {
		return err
	}
	s.closer = conn
	s.addr = conn.LocalAddr()

	if s.ReadBufferSize.Size > 0 {
		conn.SetReadBuffer(int(s.ReadBufferSize.Size))
	}

	s.Log.Infof("Listening on %s://%s", s.addr.Network(), s.addr.String())

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.read(acc, conn)
	}()

	return nil
}

// Gather is a NOOP for sFlow as it receives, asynchronously, sFlow network packets
func (s *SFlow) Gather(_ telegraf.Accumulator) error {
	return nil
}

func (s *SFlow) Stop() {
	if s.closer != nil {
		s.closer.Close()
	}
	s.wg.Wait()
}

func (s *SFlow) Address() net.Addr {
	return s.addr
}

func (s *SFlow) read(acc telegraf.Accumulator, conn net.PacketConn) {
	buf := make([]byte, maxPacketSize)
	for {
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			if !strings.HasSuffix(err.Error(), ": use of closed network connection") {
				acc.AddError(err)
			}
			break
		}
		s.cacheOpenstackPortInfo()
		s.process(acc, buf[:n])
	}
}

func (s *SFlow) cacheOpenstackPortInfo() {
	if s.decoder.LatestCache {
		return
	}

	// Open up our database connection.
	db, err := sql.Open("mysql", "root@unix(/var/lib/mysql/mysql.sock)/")
	// if there is an error opening the connection, handle it
	if err != nil {
		s.Log.Errorf("Failed to connect to openstack database: %s", err)
	}
	defer db.Close()

	// Execute the query
	ports, err := db.Query(`
select p.id,p.mac_address,p.project_id,p.device_id,i.display_name,k.name
from neutron.ports p, nova.instances i, keystone.project k
where p.device_id = i.uuid and p.project_id = k.id
`)
	if err != nil {
		s.Log.Errorf("Failed to read openstack port info: %s", err)
	}

	var portCache []PortInfo
	for ports.Next() {
		// for each row, scan the result into our tag composite object
		var id, tenantId, macAddr, instanceId, instanceName, tenantName sql.NullString
		err = ports.Scan(&id, &macAddr, &tenantId, &instanceId, &instanceName, &tenantName)
		if err != nil {
			s.Log.Errorf("Failed to fetch openstack port data: %s", err)
		}
		p := PortInfo {
			PortId: id.String,
			PortType: "fixed",
			MacAddr: macAddr.String,
			TenantId: tenantId.String,
			TenantName: tenantName.String,
			InstanceId: instanceId.String,
			InstanceName: instanceName.String,
		}
		portCache = append(portCache, p)
	}

	fips, err := db.Query(`
select p.id,p.mac_address,f.fixed_port_id
from neutron.floatingips f, neutron.ports p
where p.id = f.floating_port_id and f.id = p.device_id
`)

	for fips.Next() {
		// for each row, scan the result into our tag composite object
		var id, macAddr, portId sql.NullString
		err = fips.Scan(&id, &macAddr, &portId)
		if err != nil {
			s.Log.Errorf("Failed to fetch openstack floating ip data: %s", err)
		}
		for i := range portCache {
			p := portCache[i]
			if p.PortId == portId.String {
				f := PortInfo {
					PortId: id.String,
					PortType: "floating",
					MacAddr: macAddr.String,
					TenantId: p.TenantId,
					TenantName: p.TenantName,
					InstanceId: p.InstanceId,
					InstanceName: p.InstanceName,
				}
				portCache = append(portCache, f)
			}
		}
	}

	s.decoder.UpdatePortCache(portCache)
	s.Log.Infof("Updated Openstack Port Cache")
}

func (s *SFlow) process(acc telegraf.Accumulator, buf []byte) {

	if err := s.decoder.Decode(bytes.NewBuffer(buf)); err != nil {
		acc.AddError(fmt.Errorf("unable to parse incoming packet: %s", err))
	}
}

func listenUDP(network string, address string) (*net.UDPConn, error) {
	switch network {
	case "udp", "udp4", "udp6":
		addr, err := net.ResolveUDPAddr(network, address)
		if err != nil {
			return nil, err
		}
		return net.ListenUDP(network, addr)
	default:
		return nil, fmt.Errorf("unsupported network type: %s", network)
	}
}

// init registers this SFlow input plug in with the Telegraf framework
func init() {
	inputs.Add("sflow", func() telegraf.Input {
		return &SFlow{}
	})
}
