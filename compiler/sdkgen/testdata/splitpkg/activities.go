package splitpkg

import "context"

// @Activity(type="split.charge",queue="split")
func Charge(_ context.Context, in ChargeInput) (ChargeOutput, error) {
	return ChargeOutput{ChargeID: "charge-" + in.OrderID}, nil
}

// @Activity(type="split.record",queue="split")
func Record(_ context.Context, in RecordInput) (RecordOutput, error) {
	return RecordOutput{Receipt: "receipt-" + in.ChargeID}, nil
}

// @Activity(type="split.reverse",queue="split")
func Reverse(_ context.Context, in ReverseInput) (ReverseOutput, error) {
	return ReverseOutput{Reversed: in.ChargeID != ""}, nil
}
