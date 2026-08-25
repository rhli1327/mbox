package dhcp

import (
	"context"
	"strings"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/dns/transport"
	E "github.com/sagernet/sing/common/exceptions"

	mDNS "github.com/miekg/dns"
)

func (t *Transport) exchangeWithTransports(ctx context.Context, message *mDNS.Msg, serverTransports []adapter.DNSTransport, callback func(response *mDNS.Msg, err error)) {
	originalQuestion := message.Question[0]
	domain := dns.FqdnToDomain(originalQuestion.Name)
	names := t.nameList(domain)
	if len(names) == 0 {
		callback(nil, E.New("dhcp: invalid domain: ", domain))
		return
	}
	var nameErrorResponse *mDNS.Msg
	nameExchangers := make([]transport.AsyncExchanger, 0, len(names))
	for _, fqdn := range names {
		nameExchanger := t.newNameExchanger(message, fqdn, serverTransports)
		nameExchangers = append(nameExchangers, func(ctx context.Context, callback func(response *mDNS.Msg, err error)) {
			nameExchanger(ctx, func(response *mDNS.Msg, err error) {
				if err == nil {
					restoreOriginalQuestion(response, fqdn, originalQuestion)
					if response.Rcode == mDNS.RcodeNameError && (nameErrorResponse == nil || fqdn == originalQuestion.Name) {
						nameErrorResponse = response
					}
				}
				callback(response, err)
			})
		})
	}
	transport.ExchangeSequential(ctx, nameExchangers, func(response *mDNS.Msg, err error) bool {
		return err == nil && response.Rcode != mDNS.RcodeNameError
	}, func(response *mDNS.Msg, err error) {
		if nameErrorResponse != nil && (err != nil || response == nil || response.Rcode == mDNS.RcodeNameError) {
			callback(nameErrorResponse, nil)
			return
		}
		callback(response, err)
	})
}

// Stub resolvers discard Answer RRs whose owner name does not match the question.
func restoreOriginalQuestion(response *mDNS.Msg, fqdn string, question mDNS.Question) {
	response.Question = []mDNS.Question{question}
	for _, record := range response.Answer {
		if strings.EqualFold(record.Header().Name, fqdn) {
			record.Header().Name = question.Name
		}
	}
}

func (t *Transport) newNameExchanger(message *mDNS.Msg, fqdn string, serverTransports []adapter.DNSTransport) transport.AsyncExchanger {
	attemptExchangers := make([]transport.AsyncExchanger, 0, t.attempts*len(serverTransports))
	for range t.attempts {
		for _, serverTransport := range serverTransports {
			attemptExchangers = append(attemptExchangers, func(ctx context.Context, callback func(response *mDNS.Msg, err error)) {
				serverTransport.ExchangeAsync(ctx, transport.NewFanOutRequest(message, fqdn, true), callback)
			})
		}
	}
	return func(ctx context.Context, callback func(response *mDNS.Msg, err error)) {
		transport.ExchangeSequential(ctx, attemptExchangers, nil, func(response *mDNS.Msg, err error) {
			if err != nil {
				err = E.Cause(err, fqdn)
			}
			callback(response, err)
		})
	}
}

func (t *Transport) nameList(name string) []string {
	l := len(name)
	rooted := l > 0 && name[l-1] == '.'
	if l > 254 || l == 254 && !rooted {
		return nil
	}

	if rooted {
		if avoidDNS(name) {
			return nil
		}
		return []string{name}
	}

	hasNdots := strings.Count(name, ".") >= t.ndots
	name += "."
	// l++

	names := make([]string, 0, 1+len(t.search))
	if hasNdots && !avoidDNS(name) {
		names = append(names, name)
	}
	for _, suffix := range t.search {
		fqdn := name + suffix
		if !avoidDNS(fqdn) && len(fqdn) <= 254 {
			names = append(names, fqdn)
		}
	}
	if !hasNdots && !avoidDNS(name) {
		names = append(names, name)
	}
	return names
}

func avoidDNS(name string) bool {
	if name == "" {
		return true
	}
	if name[len(name)-1] == '.' {
		name = name[:len(name)-1]
	}
	return strings.HasSuffix(name, ".onion")
}
