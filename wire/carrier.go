package wire

// ProtobufCarrier is a DelegatingCarrier that uses protocol buffers as the
// the underlying datastructure. The reason for implementing DelagatingCarrier
// is to allow for end users to serialize the underlying protocol buffers using
// jsonpb or any other serialization forms they want.
type ProtobufCarrier struct {
	TracerState *TracerState
	Baggage     *Baggage
}

func (p *ProtobufCarrier) SetState(traceID, spanID int64, sampled bool) {
	p.TracerState.TraceId = traceID
	p.TracerState.SpanId = spanID
	p.TracerState.Sampled = sampled
}

func (p *ProtobufCarrier) State() (traceID, spanID int64, sampled bool) {
	traceID = p.TracerState.TraceId
	spanID = p.TracerState.SpanId
	sampled = p.TracerState.Sampled
	return traceID, spanID, sampled
}

func (p *ProtobufCarrier) SetBaggageItem(key, value string) {
	if p.Baggage.Items == nil {
		p.Baggage.Items = map[string]string{key: value}
		return
	}

	p.Baggage.Items[key] = value
}

func (p *ProtobufCarrier) GetBaggage(f func(k, v string)) {
	for k, v := range p.Baggage.Items {
		f(k, v)
	}
}
