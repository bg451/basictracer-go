package wire

// ProtobufCarrier is a DelegatingCarrier that uses protocol buffers as the
// the underlying datastructure. The reason for implementing DelagatingCarrier
// is to allow for end users to serialize the underlying protocol buffers using
// jsonpb or any other serialization forms they want.
type ProtobufCarrier TracerState

func (p *ProtobufCarrier) SetState(traceID, spanID int64, sampled bool) {
	p.TraceId = traceID
	p.SpanId = spanID
	p.Sampled = sampled
}

func (p *ProtobufCarrier) State() (traceID, spanID int64, sampled bool) {
	traceID = p.TraceId
	spanID = p.SpanId
	sampled = p.Sampled
	return traceID, spanID, sampled
}

func (p *ProtobufCarrier) SetBaggageItem(key, value string) {
	if p.BaggageItems == nil {
		p.BaggageItems = map[string]string{key: value}
		return
	}

	p.BaggageItems[key] = value
}

func (p *ProtobufCarrier) GetBaggage(f func(k, v string)) {
	for k, v := range p.BaggageItems {
		f(k, v)
	}
}
