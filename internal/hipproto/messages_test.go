package hipproto

import (
	"testing"
)

func TestHelloRoundTrip(t *testing.T) {
	h := Hello{
		Proto:            "hip",
		Version:          1,
		BearerToken:      "tok",
		Region:           "jp",
		AppVersion:       "6.6.0",
		AssetVersion:     "6.6.0.20",
		AssetHash:        "e1f2ec17",
		RunID:            "01J8XYZ",
		UnpackerVersion:  "6.0.5",
		ExpectedMaxFrame: DefaultMaxFrameBytes,
	}
	b, err := Encode(&h)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var back Hello
	if err := Decode(b, &back); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if back.Region != "jp" || back.AssetVersion != "6.6.0.20" || back.ExpectedMaxFrame != DefaultMaxFrameBytes {
		t.Fatalf("mismatch: %+v", back)
	}
}

func TestCheckResultRoundTrip(t *testing.T) {
	r := CheckResult{
		BatchID: 42,
		Results: []CheckAckItem{
			{Path: "a", Action: ActionSkip},
			{Path: "b", Action: ActionUpload, Placement: PlacementShared},
			{Path: "c", Action: ActionUpload, Placement: PlacementOverride},
		},
	}
	b, err := Encode(&r)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var back CheckResult
	if err := Decode(b, &back); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(back.Results) != 3 {
		t.Fatalf("len: %d", len(back.Results))
	}
	if back.Results[0].Action != ActionSkip {
		t.Fatalf("action[0]: %q", back.Results[0].Action)
	}
	if back.Results[1].Placement != PlacementShared {
		t.Fatalf("placement[1]: %q", back.Results[1].Placement)
	}
	if back.Results[2].Placement != PlacementOverride {
		t.Fatalf("placement[2]: %q", back.Results[2].Placement)
	}
}

func TestUploadAckRoundTrip(t *testing.T) {
	a := UploadAck{
		StreamID:     7,
		Status:       UploadStatusOK,
		Placement:    PlacementShared,
		ServerSha256: "deadbeef",
		StorageKey:   "/shared-assets/foo",
	}
	b, err := Encode(&a)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var back UploadAck
	if err := Decode(b, &back); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if back.Status != UploadStatusOK || back.StorageKey != "/shared-assets/foo" {
		t.Fatalf("mismatch: %+v", back)
	}
}

func TestCommitRoundTrip(t *testing.T) {
	c := Commit{
		BundleCount: 100,
		Stats: CommitStats{
			SkippedByLayer1:  10,
			SkippedByCheck:   20,
			UploadedShared:   30,
			UploadedOverride: 40,
		},
	}
	b, err := Encode(&c)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var back Commit
	if err := Decode(b, &back); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if back.BundleCount != 100 || back.Stats.UploadedOverride != 40 {
		t.Fatalf("mismatch: %+v", back)
	}
}
