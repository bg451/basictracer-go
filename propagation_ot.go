package basictracer

import (
	"bytes"
	"encoding/binary"
	"io"
	"strconv"
	"strings"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
)

type textMapPropagator struct {
	tracer *tracerImpl
}
type binaryPropagator struct {
	tracer *tracerImpl
}
type goHTTPPropagator struct {
	*textMapPropagator
}

const (
	prefixTracerState = "ot-tracer-"
	prefixBaggage     = "ot-baggage-"

	tracerStateFieldCount = 3
	fieldNameTraceID      = prefixTracerState + "traceid"
	fieldNameSpanID       = prefixTracerState + "spanid"
	fieldNameSampled      = prefixTracerState + "sampled"
)

func (p *textMapPropagator) Inject(
	sp opentracing.Span,
	opaqueCarrier interface{},
) error {
	sc, ok := sp.(*spanImpl)
	if !ok {
		return opentracing.ErrInvalidSpan
	}
	carrier, ok := opaqueCarrier.(opentracing.TextMapWriter)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}
	carrier.Set(fieldNameTraceID, strconv.FormatInt(sc.raw.TraceID, 16))
	carrier.Set(fieldNameSpanID, strconv.FormatInt(sc.raw.SpanID, 16))
	carrier.Set(fieldNameSampled, strconv.FormatBool(sc.raw.Sampled))

	sc.Lock()
	for k, v := range sc.raw.Baggage {
		carrier.Set(prefixBaggage+k, v)
	}
	sc.Unlock()
	return nil
}

func (p *textMapPropagator) Join(
	operationName string,
	opaqueCarrier interface{},
) (opentracing.Span, error) {
	carrier, ok := opaqueCarrier.(opentracing.TextMapReader)
	if !ok {
		return nil, opentracing.ErrInvalidCarrier
	}
	requiredFieldCount := 0
	var traceID, propagatedSpanID int64
	var sampled bool
	var err error
	decodedBaggage := make(map[string]string)
	err = carrier.ForeachKey(func(k, v string) error {
		switch strings.ToLower(k) {
		case fieldNameTraceID:
			traceID, err = strconv.ParseInt(v, 16, 64)
			if err != nil {
				return opentracing.ErrTraceCorrupted
			}
		case fieldNameSpanID:
			propagatedSpanID, err = strconv.ParseInt(v, 16, 64)
			if err != nil {
				return opentracing.ErrTraceCorrupted
			}
		case fieldNameSampled:
			sampled, err = strconv.ParseBool(v)
			if err != nil {
				return opentracing.ErrTraceCorrupted
			}
		default:
			lowercaseK := strings.ToLower(k)
			if strings.HasPrefix(lowercaseK, prefixBaggage) {
				decodedBaggage[strings.TrimPrefix(lowercaseK, prefixBaggage)] = v
			}
			// Balance off the requiredFieldCount++ just below...
			requiredFieldCount--
		}
		requiredFieldCount++
		return nil
	})
	if err != nil {
		return nil, err
	}
	if requiredFieldCount < tracerStateFieldCount {
		if requiredFieldCount == 0 {
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

func (p *binaryPropagator) Inject(
	sp opentracing.Span,
	opaqueCarrier interface{},
) error {
	sc, ok := sp.(*spanImpl)
	if !ok {
		return opentracing.ErrInvalidSpan
	}
	carrier, ok := opaqueCarrier.(io.Writer)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}
	var err error
	var sampledByte byte
	if sc.raw.Sampled {
		sampledByte = 1
	}

	// Handle the trace and span ids, and sampled status.
	err = binary.Write(carrier, binary.BigEndian, sc.raw.TraceID)
	if err != nil {
		return err
	}

	err = binary.Write(carrier, binary.BigEndian, sc.raw.SpanID)
	if err != nil {
		return err
	}

	err = binary.Write(carrier, binary.BigEndian, sampledByte)
	if err != nil {
		return err
	}

	// Handle the baggage.
	err = binary.Write(carrier, binary.BigEndian, int32(len(sc.raw.Baggage)))
	if err != nil {
		return err
	}
	for key, val := range sc.raw.Baggage {
		if err = binary.Write(carrier, binary.BigEndian, int32(len(key))); err != nil {
			return err
		}
		if _, err = io.WriteString(carrier, key); err != nil {
			return err
		}

		if err = binary.Write(carrier, binary.BigEndian, int32(len(val))); err != nil {
			return err
		}
		if _, err = io.WriteString(carrier, val); err != nil {
			return err
		}
	}

	return nil
}

func (p *binaryPropagator) Join(
	operationName string,
	opaqueCarrier interface{},
) (opentracing.Span, error) {
	carrier, ok := opaqueCarrier.(io.Reader)
	if !ok {
		return nil, opentracing.ErrInvalidCarrier
	}
	// Handle the trace, span ids, and sampled status.
	var traceID, propagatedSpanID int64
	var sampledByte byte

	if err := binary.Read(carrier, binary.BigEndian, &traceID); err != nil {
		if err == io.EOF {
			return nil, opentracing.ErrTraceNotFound
		}
		return nil, opentracing.ErrTraceCorrupted
	}
	if err := binary.Read(carrier, binary.BigEndian, &propagatedSpanID); err != nil {
		return nil, opentracing.ErrTraceCorrupted
	}
	if err := binary.Read(carrier, binary.BigEndian, &sampledByte); err != nil {
		return nil, opentracing.ErrTraceCorrupted
	}

	// Handle the baggage.
	var numBaggage int32
	if err := binary.Read(carrier, binary.BigEndian, &numBaggage); err != nil {
		return nil, opentracing.ErrTraceCorrupted
	}
	iNumBaggage := int(numBaggage)
	var baggageMap map[string]string
	if iNumBaggage > 0 {
		var buf bytes.Buffer // TODO(tschottdorf): candidate for sync.Pool
		baggageMap = make(map[string]string, iNumBaggage)
		var keyLen, valLen int32
		for i := 0; i < iNumBaggage; i++ {
			if err := binary.Read(carrier, binary.BigEndian, &keyLen); err != nil {
				return nil, opentracing.ErrTraceCorrupted
			}
			buf.Grow(int(keyLen))
			if n, err := io.CopyN(&buf, carrier, int64(keyLen)); err != nil || int32(n) != keyLen {
				return nil, opentracing.ErrTraceCorrupted
			}
			key := buf.String()
			buf.Reset()

			if err := binary.Read(carrier, binary.BigEndian, &valLen); err != nil {
				return nil, opentracing.ErrTraceCorrupted
			}
			if n, err := io.CopyN(&buf, carrier, int64(valLen)); err != nil || int32(n) != valLen {
				return nil, opentracing.ErrTraceCorrupted
			}
			baggageMap[key] = buf.String()
			buf.Reset()
		}
	}

	sp := p.tracer.getSpan()
	sp.raw = RawSpan{
		Context: Context{
			TraceID:      traceID,
			SpanID:       randomID(),
			ParentSpanID: propagatedSpanID,
			Sampled:      sampledByte != 0,
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
