package basictracer

import (
	"encoding/base64"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/opentracing/basictracer-go/wire"
	opentracing "github.com/opentracing/opentracing-go"
)

type splitTextPropagator struct {
	tracer *tracerImpl
}
type splitBinaryPropagator struct {
	tracer *tracerImpl
}
type goHTTPPropagator struct {
	*splitBinaryPropagator
}

const (
	prefixTracerState = "ot-tracer-"
	prefixBaggage     = "ot-baggage-"

	tracerStateFieldCount = 3
	fieldNameTraceID      = prefixTracerState + "traceid"
	fieldNameSpanID       = prefixTracerState + "spanid"
	fieldNameSampled      = prefixTracerState + "sampled"
)

func (p *splitTextPropagator) Inject(
	sp opentracing.Span,
	carrier interface{},
) error {
	sc, ok := sp.(*spanImpl)
	if !ok {
		return opentracing.ErrInvalidSpan
	}
	splitTextCarrier, ok := carrier.(*opentracing.SplitTextCarrier)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}
	if splitTextCarrier.TracerState == nil {
		splitTextCarrier.TracerState = make(map[string]string, tracerStateFieldCount)
	}
	splitTextCarrier.TracerState[fieldNameTraceID] = strconv.FormatInt(sc.raw.TraceID, 16)
	splitTextCarrier.TracerState[fieldNameSpanID] = strconv.FormatInt(sc.raw.SpanID, 16)
	splitTextCarrier.TracerState[fieldNameSampled] = strconv.FormatBool(sc.raw.Sampled)

	sc.Lock()
	if l := len(sc.raw.Baggage); l > 0 && splitTextCarrier.Baggage == nil {
		splitTextCarrier.Baggage = make(map[string]string, l)
	}
	for k, v := range sc.raw.Baggage {
		splitTextCarrier.Baggage[prefixBaggage+k] = v
	}
	sc.Unlock()
	return nil
}

func (p *splitTextPropagator) Join(
	operationName string,
	carrier interface{},
) (opentracing.Span, error) {
	splitTextCarrier, ok := carrier.(*opentracing.SplitTextCarrier)
	if !ok {
		return nil, opentracing.ErrInvalidCarrier
	}
	requiredFieldCount := 0
	var traceID, propagatedSpanID int64
	var sampled bool
	var err error
	for k, v := range splitTextCarrier.TracerState {
		switch strings.ToLower(k) {
		case fieldNameTraceID:
			traceID, err = strconv.ParseInt(v, 16, 64)
			if err != nil {
				return nil, opentracing.ErrTraceCorrupted
			}
		case fieldNameSpanID:
			propagatedSpanID, err = strconv.ParseInt(v, 16, 64)
			if err != nil {
				return nil, opentracing.ErrTraceCorrupted
			}
		case fieldNameSampled:
			sampled, err = strconv.ParseBool(v)
			if err != nil {
				return nil, opentracing.ErrTraceCorrupted
			}
		default:
			continue
		}
		requiredFieldCount++
	}
	var decodedBaggage map[string]string
	if splitTextCarrier.Baggage != nil {
		decodedBaggage = make(map[string]string)
		for k, v := range splitTextCarrier.Baggage {
			lowercaseK := strings.ToLower(k)
			if strings.HasPrefix(lowercaseK, prefixBaggage) {
				decodedBaggage[strings.TrimPrefix(lowercaseK, prefixBaggage)] = v
			}
		}
	}
	if requiredFieldCount < tracerStateFieldCount {
		if len(splitTextCarrier.TracerState) == 0 {
			return nil, opentracing.ErrTraceNotFound
		}
		return nil, opentracing.ErrTraceCorrupted
	}

	sp := p.tracer.getSpan()
	sp.raw = RawSpan{
		Context: Context{
			TraceID:      traceID,
			SpanID:       randomID(),
			ParentSpanID: propagatedSpanID,
			Sampled:      sampled,
		},
		Baggage: decodedBaggage,
	}

	return p.tracer.startSpanInternal(
		sp,
		operationName,
		time.Now(),
		nil,
	), nil
}

func (p *splitBinaryPropagator) Inject(
	sp opentracing.Span,
	carrier interface{},
) error {
	sc, ok := sp.(*spanImpl)
	if !ok {
		return opentracing.ErrInvalidSpan
	}
	splitBinaryCarrier, ok := carrier.(*opentracing.SplitBinaryCarrier)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}

	state := wire.TracerState{}
	state.TraceId = sc.raw.TraceID
	state.SpanId = sc.raw.SpanID
	state.Sampled = sc.raw.Sampled

	contextBytes, err := proto.Marshal(&state)
	if err != nil {
		return err
	}
	splitBinaryCarrier.TracerState = contextBytes

	// Only attempt to encode the baggage if it has items.
	if len(sc.raw.Baggage) > 0 {
		sc.Lock()
		baggage := wire.Baggage{}
		baggage.Items = sc.raw.Baggage

		baggageBytes, err := proto.Marshal(&baggage)
		sc.Unlock()
		if err != nil {
			return err
		}
		splitBinaryCarrier.Baggage = baggageBytes
	}

	return nil
}

func (p *splitBinaryPropagator) Join(
	operationName string,
	carrier interface{},
) (opentracing.Span, error) {
	splitBinaryCarrier, ok := carrier.(*opentracing.SplitBinaryCarrier)
	if !ok {
		return nil, opentracing.ErrInvalidCarrier
	}
	if len(splitBinaryCarrier.TracerState) == 0 {
		return nil, opentracing.ErrTraceNotFound
	}

	// Handle the trace, span ids, and sampled status.
	ctx := wire.TracerState{}
	if err := proto.Unmarshal(splitBinaryCarrier.TracerState, &ctx); err != nil {
		return nil, opentracing.ErrTraceCorrupted
	}

	var baggageMap map[string]string

	// Only try to decode the baggage if it has data.
	if len(splitBinaryCarrier.Baggage) > 0 {
		baggage := wire.Baggage{}
		if err := proto.Unmarshal(splitBinaryCarrier.Baggage, &baggage); err != nil {
			return nil, opentracing.ErrTraceCorrupted
		}
		baggageMap = baggage.Items
	}

	sp := p.tracer.getSpan()
	sp.raw = RawSpan{
		Context: Context{
			TraceID:      ctx.TraceId,
			SpanID:       randomID(),
			ParentSpanID: ctx.SpanId,
			Sampled:      ctx.Sampled,
		},
	}

	sp.raw.Baggage = baggageMap

	return p.tracer.startSpanInternal(
		sp,
		operationName,
		time.Now(),
		nil,
	), nil
}

const (
	tracerStateHeaderName  = "Tracer-State"
	traceBaggageHeaderName = "Trace-Baggage"
)

func (p *goHTTPPropagator) Inject(
	sp opentracing.Span,
	carrier interface{},
) error {
	// Defer to SplitBinary for the real work.
	splitBinaryCarrier := opentracing.NewSplitBinaryCarrier()
	if err := p.splitBinaryPropagator.Inject(sp, splitBinaryCarrier); err != nil {
		return err
	}

	// Encode into the HTTP header as two base64 strings.
	header := carrier.(http.Header)
	header.Add(tracerStateHeaderName, base64.StdEncoding.EncodeToString(
		splitBinaryCarrier.TracerState))
	header.Add(traceBaggageHeaderName, base64.StdEncoding.EncodeToString(
		splitBinaryCarrier.Baggage))

	return nil
}

func (p *goHTTPPropagator) Join(
	operationName string,
	carrier interface{},
) (opentracing.Span, error) {
	// Decode the two base64-encoded data blobs from the HTTP header.
	header := carrier.(http.Header)
	tracerStateBase64, found := header[http.CanonicalHeaderKey(tracerStateHeaderName)]
	if !found || len(tracerStateBase64) == 0 {
		return nil, opentracing.ErrTraceNotFound
	}
	traceBaggageBase64, found := header[http.CanonicalHeaderKey(traceBaggageHeaderName)]
	if !found || len(traceBaggageBase64) == 0 {
		return nil, opentracing.ErrTraceNotFound
	}
	tracerStateBinary, err := base64.StdEncoding.DecodeString(tracerStateBase64[0])
	if err != nil {
		return nil, opentracing.ErrTraceCorrupted
	}
	traceBaggageBinary, err := base64.StdEncoding.DecodeString(traceBaggageBase64[0])
	if err != nil {
		return nil, opentracing.ErrTraceCorrupted
	}

	// Defer to SplitBinary for the real work.
	splitBinaryCarrier := &opentracing.SplitBinaryCarrier{
		TracerState: tracerStateBinary,
		Baggage:     traceBaggageBinary,
	}
	return p.splitBinaryPropagator.Join(operationName, splitBinaryCarrier)
}
