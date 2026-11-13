package app

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"

	"github.com/donutnomad/taskq/matching"
	"github.com/donutnomad/taskq/membership"
)

// matchingConfig 把 TaskQ 配置映射成 matching.Config。listenAddr 是已经确定的实际监听地址
// （含实际端口）；广播地址见 advertisedAddr，写入 membership 表供其它副本与 frontend 拨号。
func matchingConfig(cfg Config, listenAddr string, monitor membership.Monitor, store matching.Store) (matching.Config, error) {
	broadcast, err := advertisedAddr(cfg.TaskQ.Matching, listenAddr)
	if err != nil {
		return matching.Config{}, err
	}
	return matching.Config{
		Store:         store,
		Monitor:       monitor,
		ServiceName:   cfg.TaskQ.ServiceName,
		ListenAddr:    listenAddr,
		BroadcastAddr: broadcast,
		LeaseOwner:    cfg.NodeID,
	}, nil
}

// advertisedAddr 是写进 membership 表的广播地址：配置了 TASKQ_MATCHING_ADVERTISE_ADDR 时取它，
// 否则由实际监听地址推导。两者都按 broadcastAddr 把 host 解析成 IP。
func advertisedAddr(cfg MatchingConfig, listenAddr string) (string, error) {
	if cfg.AdvertiseAddr != "" {
		return broadcastAddr(cfg.AdvertiseAddr)
	}
	return broadcastAddr(listenAddr)
}

// broadcastAddr 由实际监听地址推导 membership 广播地址。membership 表只存 IP，因此：
// host 是具体 IP 时原样使用；host 为空或未指定地址（`:port`、`[IP地址]:port`、`[::]:port`）
// 时取本机的非回环 IP；host 是本机主机名时解析为 IP。
func broadcastAddr(listenAddr string) (string, error) {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return "", fmt.Errorf("TASKQ_MATCHING_ADDR %q: %w", listenAddr, err)
	}
	ip, err := netip.ParseAddr(host)
	if host == "" || (err == nil && ip.IsUnspecified()) {
		if ip, err = localIP(); err != nil {
			return "", err
		}
	} else if err != nil {
		ip, err = resolveHostIP(host)
		if err != nil {
			return "", err
		}
	}
	return net.JoinHostPort(ip.String(), port), nil
}

func resolveHostIP(host string) (netip.Addr, error) {
	addrs, err := net.LookupIP(host)
	if err != nil || len(addrs) == 0 {
		return netip.Addr{}, fmt.Errorf("resolve TASKQ_MATCHING_ADDR host %q: %w", host, err)
	}
	ip, ok := netip.AddrFromSlice(addrs[0])
	if !ok {
		return netip.Addr{}, fmt.Errorf("resolve TASKQ_MATCHING_ADDR host %q: unusable address %s", host, addrs[0])
	}
	return ip.Unmap(), nil
}

// localIP 返回本机对外可达的 IP：优先第一个非回环、非链路本地的 IPv4，其次 IPv6。
// 只在本机完全没有可用网卡时回落到回环地址。
func localIP() (netip.Addr, error) {
	interfaces, err := net.InterfaceAddrs()
	if err != nil {
		return netip.Addr{}, fmt.Errorf("list local interfaces for matching broadcast address: %w", err)
	}
	var fallback netip.Addr
	for _, addr := range interfaces {
		ipNet, ok := addr.(*net.IPNet)
		if !ok || ipNet.IP.IsLoopback() || ipNet.IP.IsLinkLocalUnicast() {
			continue
		}
		ip, ok := netip.AddrFromSlice(ipNet.IP)
		if !ok {
			continue
		}
		ip = ip.Unmap()
		if ip.Is4() {
			return ip, nil
		}
		if !fallback.IsValid() {
			fallback = ip
		}
	}
	if fallback.IsValid() {
		return fallback, nil
	}
	loopback, _ := netip.AddrFromSlice(net.IPv4(127, 0, 0, 1))
	return loopback, nil
}

// validateMatchingAddr 在启动期拦下无法用于监听的地址。
func validateMatchingAddr(addr string) error {
	if strings.TrimSpace(addr) == "" {
		return fmt.Errorf("TASKQ_MATCHING_ADDR is empty")
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return fmt.Errorf("TASKQ_MATCHING_ADDR %q: %w", addr, err)
	}
	if _, err := broadcastAddr(addr); err != nil {
		return err
	}
	return nil
}

// validateAdvertiseAddr 校验 TASKQ_MATCHING_ADVERTISE_ADDR：空表示不配置；否则必须是 host:port，
// 端口不能为 0（其他副本按它拨号）。
func validateAdvertiseAddr(addr string) error {
	if addr == "" {
		return nil
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("TASKQ_MATCHING_ADVERTISE_ADDR %q: %w", addr, err)
	}
	if n, err := strconv.Atoi(port); err != nil || n <= 0 || n > 65535 {
		return fmt.Errorf("TASKQ_MATCHING_ADVERTISE_ADDR %q: port must be 1-65535", addr)
	}
	if _, err := broadcastAddr(addr); err != nil {
		return fmt.Errorf("TASKQ_MATCHING_ADVERTISE_ADDR: %w", err)
	}
	return nil
}
